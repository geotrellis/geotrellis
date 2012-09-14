package geotrellis.data

import org.scalatest.FunSpec
import org.scalatest.matchers.MustMatchers

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ColorSpec extends FunSpec with MustMatchers {
  describe("LinearColorRangeChooser(FF0000,0000FF)") {
    val c = new LinearColorRangeChooser(0xFF0000, 0x0000FF)
    val c2 = new MultiColorRangeChooser(Array(0xFF0000, 0x0000FF))

    it("should provide 1 color") {
      c.getColorString(1) must be === "FF0000"
      c2.getColorString(1) must be === "FF0000"
    }

    it("should provide 2 colors") {
      c.getColorString(2) must be === "FF0000,0000FF"
      c2.getColorString(2) must be === "FF0000,0000FF"
    }

    it("should provide 3 colors") {
      c.getColorString(3) must be === "FF0000,80007F,0000FF"
      c2.getColorString(3) must be === "FF0000,80007F,0000FF"
    }

    it("should provide 9 colors") {
      c.getColorString(9) must be === "FF0000,E0001F,C0003F,A0005F,80007F,60009F,4000BF,2000DF,0000FF"
      c2.getColorString(9) must be === "FF0000,E0001F,C0003F,A0005F,80007F,60009F,4000BF,2000DF,0000FF"
    }

    it("should unzip colors") {
      val n = 0xff9900ff
      val (r, g, b, a) = Color.unzip(n)
      println("n=%s, r=%s g=%s b=%s a=%s" format (n, r, g, b, a))
      r must be === 0xff
      g must be === 0x99
      b must be === 0x00
      a must be === 0xff
    }
  }

  describe("MultiColorRangeChooser()") {
    it("should work 1") {
      val baseColors = Array(0x0000ff)
      val chooser = new MultiColorRangeChooser(baseColors)
      val colors = chooser.getColors(baseColors.length)
      colors.toList must be === baseColors.toList
    }

    it("should work 2") {
      val baseColors = Array(0x0000ff, 0xff0000)
      val chooser = new MultiColorRangeChooser(baseColors)
      val colors = chooser.getColors(baseColors.length)
      colors.toList must be === baseColors.toList
    }

    it("should work 3") {
      val baseColors = Array(0x0000ff, 0x00ff00, 0xff0000)
      val chooser = new MultiColorRangeChooser(baseColors)
      val colors = chooser.getColors(baseColors.length)
      colors.map{ "%06x".format(_) }.toList must be === baseColors.map { "%06x".format(_) }.toList
    }

    it("should work 6") {
      val baseColors = Array(0x0000ff, 0x0080ff, 0x00ff80, 0xffff00, 0xff8000, 0xff0000)
      val chooser = new MultiColorRangeChooser(baseColors)
      val colors = chooser.getColors(baseColors.length)
      colors.toList must be === baseColors.toList
    }
  }

  describe("ColorBreaks") {
    it("should map breaks to colors") {
      val limits = Array(2, 4, 6)
      val colors = Array(0xff0000ff, 0x00ff00ff, 0x0000ffff)
      val cb = ColorBreaks(limits, colors)
      cb.limits must be === limits
      cb.colors must be === colors
    }
  }
}
