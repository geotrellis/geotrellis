package geotrellis.osm

import geotrellis.util._
import geotrellis.vector._

import com.vividsolutions.jts.operation.linemerge.LineMerger
import com.vividsolutions.jts.geom.LineString
import org.apache.spark.rdd._

// --- //

class ElementToFeatureRDDMethods(val self: RDD[Element]) extends MethodExtensions[RDD[Element]] {
  def toFeatures: RDD[OSMFeature] = {

    /* All OSM Nodes */
    val nodes: RDD[Node] = self.flatMap({
      case e: Node => Some(e)
      case _ => None
    })

    /* All OSM Ways */
    val ways: RDD[Way] = self.flatMap({
      case e: Way  => Some(e)
      case _ => None
    })

    // Inefficient to do the flatMap thrice!
    // A function `split: RDD[Element] => (RDD[Node], RDD[Way], RDD[Relation])
    // would be nice.
    /* All OSM Relations */
    val relations: RDD[Relation] = self.flatMap({
      /* Baby steps. Limit to handling multipolys for now */
      case e: Relation if e.data.tagMap.get("type") == Some("multipolygon") => Some(e)
      case _ => None
    })

    val (points, lines, polys) = geometries(nodes, ways)

    val (finalPolys, finalLines) = multipolygons(lines, polys, relations)

    points.asInstanceOf[RDD[OSMFeature]] ++
    finalLines.asInstanceOf[RDD[OSMFeature]] ++
    finalPolys.asInstanceOf[RDD[OSMFeature]]
  }

  private def multipolygons(
    lines: RDD[Feature[Line, Tree[ElementData]]],
    polys: RDD[Feature[Polygon, Tree[ElementData]]],
    relations: RDD[Relation]
  ): (RDD[Feature[MultiPolygon, Tree[ElementData]]], RDD[Feature[Line, Tree[ElementData]]]) = {
    // filter out polys that are used in relations
    // merge RDDs back together

    val relLinks: RDD[(Long, Relation)] =
      relations.flatMap(r => r.members.map(m => (m.ref, r)))

    val lineLinks: RDD[(Long, Feature[Line, Tree[ElementData]])] =
      lines.map(f => (f.data.root.meta.id, f))

    val grouped =
      polys.map(f => (f.data.root.meta.id, f)).cogroup(lineLinks, relLinks)

    val multipolys = grouped
      /* Assumption: Polygons and Lines exist in at most one "multipolygon" Relation */
      .flatMap({
        case (_, (ps, _, rs)) if !rs.isEmpty && !ps.isEmpty => Some((rs.head, Left(ps.head)))
        case (_, (_, ls, rs)) if !rs.isEmpty && !ls.isEmpty => Some((rs.head, Right(ls.head)))
        case _ => None
      })
      .groupByKey
      .map({ case (r, gs) =>

        /* Fuse Lines into Polygons */
        val ls: Vector[Feature[Line, Tree[ElementData]]] = gs.flatMap({
          case Right(l) => Some(l)
          case _ => None
        }).toVector

        val ps: Vector[Feature[Polygon, Tree[ElementData]]] = gs.flatMap({
          case Left(p) => Some(p)
          case _ => None
        }).toVector ++ fuseLines(spatialSort(ls.map(f => (f.geom.centroid.as[Point].get, f))).map(_._2))

        val outerIds: Set[Long] = r.members.partition(_.role == "outer")._1.map(_.ref).toSet

        val (outers, inners) = ps.partition(f => outerIds.contains(f.data.root.meta.id))

        /* Match outer and inner Polygons - O(n^2) */
        val fused = outers.map({ o =>
          Polygon(
            o.geom.exterior,
            inners.filter(i => o.geom.contains(i.geom)).map(_.geom.exterior)
          )
        })

        /* It is suggested by OSM that multipoly tag data should be stored in
         * the Relation, not its constituent parts. Hence we take `r.data`
         * as the root `ElementData` here.
         *
         * However, "inner" Ways can have meaningful tags, such as a lake in
         * the middle of a forest.
         *
         * Furthermore, winding order doesn't matter in OSM, but it does
         * in VectorTiles.
         * TODO: Make sure winding order is handled correctly.
         */
        Feature(MultiPolygon(fused), Tree(r.data, outers.map(_.data) ++ inners.map(_.data)))
      })

    /* Lines which were part of no Relation */
    val openLines = grouped.flatMap({
      case (_, (ps, ls, rs)) if ps.isEmpty && rs.isEmpty => Some(ls.head)
      case _ => None
    })

    (multipolys, openLines)
  }

  /** Order a given Vector of Features such that each Geometry is as
    * spatially close as possible to its neighbours in the result Vector.
    * This is done by comparing the location of a centroid Point given for
    * each Geometry.
    *
    * Time complexity: O(nlogn)
    */
  private def spatialSort[T](v: Vector[(Point, T)]): Vector[(Point, T)] = v match {
    case Vector() => v
    case v if v.length < 6 => v.sortBy(_._1.x)  // TODO Naive?
    case v => {
      /* Kernels - Two points around which to group all others */
      val mid: Point = v(v.length / 2)._1
      val avg: (Point, Point) => Point = (p1, p2) => Point((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)
      val (kl, kr) = (avg(v.head._1, mid), avg(v.last._1, mid))

      /* Group all points spatially around the two kernels */
      val (l, r) = v.partition({ f => f._1.distance(kl) < f._1.distance(kr) })

      /* Recombine the sorted lines by whichever endpoints are closest */
      (spatialSort(l), spatialSort(r)) match {
        case (Vector(), v2) => v2
        case (v1, Vector()) => v1
        case (v1, v2) => {
          val pairs = Seq(
            0 -> v1.last._1.distance(v2.head._1),
            1 -> v1.last._1.distance(v2.last._1),
            2 -> v1.head._1.distance(v2.head._1),
            3 -> v1.head._1.distance(v2.last._1)
          )

          pairs.sortBy(_._2).head._1 match {
            case 0 => v1 ++ v2
            case 1 => v1 ++ v2.reverse
            case 2 => v1.reverse ++ v2
            case 3 => v2 ++ v1
          }
        }
      }
    }
  }

  /** ASSUMPTIONS:
    *   - Every Line in the given Vector can be fused
    *   - The final result of all fusions will be a set of Polygons
    *
    * Time complexity (raw): O(n^2)
    *
    * Time complexity (sorted): O(n)
    */
  private def fuseLines(
    v: Vector[Feature[Line, Tree[ElementData]]]
  ): Vector[Feature[Polygon, Tree[ElementData]]] = v match {
    case Vector() => Vector.empty
    case v if v.length == 1 => throw new IllegalArgumentException("Single unfusable Line remaining.")
    case v => {
      val (f, d, rest) = fuseOne(v)

      if (f.isClosed)
        Feature(Polygon(f), Tree(d.head.root, d)) +: fuseLines(rest)
      else
        fuseLines(Feature(f, Tree(d.head.root, d)) +: rest)
    }
  }

  /** Fuse the head Line in the Vector with the first other Line possible.
    * This borrows [[fuseLines]]'s assumptions.
    */
  private def fuseOne(
    v: Vector[Feature[Line, Tree[ElementData]]]
  ): (Line, Seq[Tree[ElementData]], Vector[Feature[Line, Tree[ElementData]]]) = {
    val h = v.head
    val t = v.tail

    // TODO: Use a `while` instead?
    for ((f, i) <- t.zipWithIndex) {
      if (h.geom.touches(f.geom)) { /* Found two lines that should fuse */
        val lm = new LineMerger  /* from JTS */

        lm.add(h.geom.jtsGeom)
        lm.add(f.geom.jtsGeom)

        val line: Line = Line(lm.getMergedLineStrings.toArray(Array.empty[LineString]).head)

        val (a, b) = t.splitAt(i)

        /* Return early */
        return (line, Seq(h.data, f.data), a ++ b.tail)
      }
    }

    /* As every Line _must_ fuse, this should never be reached */
    ???
  }

  /** Every OSM Node and Way converted to GeoTrellis Geometries.
    * This includes Points, Lines, and Polygons which have no holes.
    * Holed polygons are handled by [[multipolygons]], as they are represented
    * by OSM Relations.
    */
  private def geometries(
    nodes: RDD[Node],
    ways: RDD[Way]
  ): (
    RDD[Feature[Point, Tree[ElementData]]],
    RDD[Feature[Line, Tree[ElementData]]],
    RDD[Feature[Polygon, Tree[ElementData]]]
  ) = {
    /* You're a long way from finishing this operation. */
    val links: RDD[(Long, Way)] = ways.flatMap(w => w.nodes.map(n => (n, w)))

    val grouped: RDD[(Long, (Iterable[Node], Iterable[Way]))] =
      nodes.map(n => (n.data.meta.id, n)).cogroup(links)

    val linesPolys: RDD[Either[Feature[Line, Tree[ElementData]], Feature[Polygon, Tree[ElementData]]]] =
      grouped
        .flatMap({ case (_, (ns, ws)) =>
          val n = ns.head

          ws.map(w => (w, n))
        })
        .groupByKey
        .map({ case (w, ns) =>
          /* De facto maximum of 2000 Nodes */
          val sorted: Vector[Node] = ns.toVector.sortBy(n => n.data.meta.id)

          /* `get` is safe, the BTree is guaranteed to be populated */
          val tree: BTree[Node] = BTree.fromSortedSeq(sorted).get

          /* A binary search branch predicate */
          val pred: (Long, BTree[Node]) => Either[Option[BTree[Node]], Node] = { (n, tree) =>
            if (n == tree.value.data.meta.id) {
              Right(tree.value)
            } else if (n < tree.value.data.meta.id) {
              Left(tree.left)
            } else {
              Left(tree.right)
            }
          }

          /* The actual node coordinates in the correct order */
          val (points, data) =
            w.nodes
              .flatMap(n => tree.searchWith(n, pred))
              .map(n => ((n.lat, n.lon), n.data))
              .unzip

          if (w.isLine) {
            Left(Feature(Line(points), Tree(w.data, data.map(d => Tree.singleton(d)))))
          } else {
            Right(Feature(Polygon(points), Tree(w.data, data.map(d => Tree.singleton(d)))))
          }
        })

    // TODO: More inefficient RDD splitting.
    val lines: RDD[Feature[Line, Tree[ElementData]]] = linesPolys.flatMap({
      case Left(l) => Some(l)
      case _ => None
    })

    val polys: RDD[Feature[Polygon, Tree[ElementData]]] = linesPolys.flatMap({
      case Right(p) => Some(p)
      case _ => None
    })

    /* Single Nodes unused in any Way */
    val points: RDD[Feature[Point, Tree[ElementData]]] = grouped.flatMap({ case (_, (ns, ws)) =>
      if (ws.isEmpty) {
        val n = ns.head

        Some(Feature(Point(n.lat, n.lon), Tree.singleton(n.data)))
      } else {
        None
      }
    })

    (points, lines, polys)
  }
}
