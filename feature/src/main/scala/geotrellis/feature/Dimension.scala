/*******************************************************************************
 * Copyright (c) 2014 Azavea.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package geotrellis.feature

import com.vividsolutions.jts.{geom => jts}

sealed trait Dimensions { private[feature] val geom: jts.Geometry }

trait AtLeastOneDimensions extends Dimensions
trait AtMostOneDimensions extends Dimensions

trait ZeroDimensions extends Dimensions 
                        with AtMostOneDimensions

trait OneDimensions extends Dimensions 
                       with AtMostOneDimensions
                       with AtLeastOneDimensions

trait TwoDimensions extends Dimensions
                       with AtLeastOneDimensions
