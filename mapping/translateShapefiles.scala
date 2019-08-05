import org.apache.spark.sql._
import com.vividsolutions.jts.geom.{Coordinate, Envelope, GeometryFactory}
import org.apache.spark.serializer.KryoSerializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.{ShapefileRDD, ShapefileReader}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD, SpatialRDD}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}

var session= SparkSession.builder().appName("GeoSparkSQL-demo").getOrCreate()

// Read in the state-wide shapefile and repackage as a .json DF
val stateShapefile="/user/cjg507/sparkproject/geometries/states"
val stateRDD = ShapefileReader.readToGeometryRDD(sc, stateShapefile)
val stateDFNoSchema = Adapter.toDf(stateRDD,session)
stateDFNoSchema.show()
stateDFNoSchema.coalesce(1).write.mode("overwrite").format("json").save(stateShapefile + "/stateGeom.json")

// Read in the county-wide shapefile and repackage as a .json DF
val countyShapefile="/user/cjg507/sparkproject/geometries/county"
val countyRDD = ShapefileReader.readToGeometryRDD(sc, countyShapefile)
val countyDFNoSchema = Adapter.toDf(countyRDD,session)
countyDFNoSchema.show()
countyDFNoSchema.coalesce(1).write.mode("overwrite").format("json").save(countyShapefile + "/countyGeom.json")







