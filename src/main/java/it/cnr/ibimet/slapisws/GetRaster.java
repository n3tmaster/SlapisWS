package it.cnr.ibimet.slapisws;


import it.cnr.ibimet.dbutils.ChartParams;
import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.cnr.ibimet.dbutils.TableSchema;
import it.lr.libs.DBManager;


import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.ogr.DataSource;
import org.gdal.ogr.Driver;
import org.gdal.ogr.ogr;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import javax.imageio.ImageIO;
import javax.ws.rs.*;
import javax.ws.rs.core.Application;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.DataBufferByte;
import java.io.*;
import java.sql.SQLException;
import java.util.*;






/**
 * Created by lerocchi on 16/02/17.
 *
 * GetRaster
 *
 * retrieves raster data from postgis
 */
@Path("/download")
public class GetRaster  extends Application implements SWH4EConst, ReclassConst{



    final static String MOBILE_STAT = "M";
    final static String FIXED_STAT = "T";
    final static String EDDY_STAT = "E";
    final static String GEOM_COL = "ST_asKML(a.the_geom, 15) as tgeom,";



    final static String COORD_COLS = "ST_X(ST_Transform(the_geom,4326)) as coordx,ST_Y(ST_Transform(the_geom,4326)) as coordy,";


    final static String MOBILE_DATA_TABLE = "dati";
    final static String FIXED_DATA_TABLE = "dati_stazioni_fisse";
    final static String EDDY_DATA_TABLE = "dati_eddy";





    @GET
    @Path("/j_get_size")
    public Response getSize(@QueryParam("table_name") String tname,
                                       @QueryParam("polygon") String polygon,
                                       @QueryParam("srid") String srid) {
        TDBManager tdb=null;

        String bounds="",bounds_out="";

        try {
            String legend;


            ByteArrayOutputStream is;


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            sqlString=" select ST_Width(ST_Union(rast)), ST_Height(ST_Union(rast)) " +
                    "from "+tname+" " +
                    "where ST_Contains(ST_GeomFromText('"+ polygon+"',"+srid+"), " +
                    "ST_Polygon(rast))";



            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {

                bounds_out = "[";
                bounds_out += tdb.getString(1);


                bounds_out += ",";


                bounds_out += tdb.getString(2);
                bounds_out += "]";

            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }

        Response.ResponseBuilder responseBuilder = Response.ok(bounds_out);

        return responseBuilder.build();
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/j_get_extent/{image_type}/{year}/{doy}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response extractWholeExtent(@PathParam("image_type") String image_type,
                                       @PathParam("year") String year,
                                       @PathParam("doy") String doy,
                                       @PathParam("polygon") String polygon,
                                       @PathParam("srid_from") String srid_from) {
        TDBManager tdb=null;

        String bounds="",bounds_out="";

        try {
            String legend;


            ByteArrayOutputStream is;


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Check for seasonal request
            if(image_type.matches(CRU_IMAGE.toLowerCase())){

                sqlString = "select ST_AsText(ST_Envelope(postgis.calculate_seasonal_forecast_spi3(" +
                        "ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+srid_from.split("/")[2]+"),"+DBSRID+"),"+year+","+doy+",'"+image_type+"')))";

            }else{
                sqlString = "select ST_AsText(ST_Envelope(" +
                        "(select ST_Clip(ST_Union(rast),ST_Transform(ST_GeomFromText('"+ polygon.split("/")[2]+"',"+srid_from.split("/")[2]+"),"+DBSRID+"), true) " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy+" " +
                        "and   ST_Intersects(rast,ST_Transform(ST_GeomFromText('"+ polygon.split("/")[2]+"',"+srid_from.split("/")[2]+"),"+DBSRID+")))))";
            }







            System.out.println(sqlString);


            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {
                bounds = tdb.getString(1);


            }



            System.out.println("Bounds_out: "+bounds);
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }

        //Response.ResponseBuilder responseBuilder = Response.ok(bounds_out).status(Response.Status.OK);

        return Response.status(200).entity(bounds).build(); //responseBuilder.build();
    }





    @GET
    @Produces("image/png")
    @Path("/j_extract_png")
    public Response extractRasterPng(@QueryParam("table_name") String tname,
                                  @QueryParam("polygon") String polygon,
                                  @QueryParam("srid") String srid,
                                  @QueryParam("srid_to") String srid2,
                                  @QueryParam("streamed") String streamed){
        TDBManager tdb=null;
        byte[] imgOut=null;

        try {

            String legend;


            ByteArrayOutputStream is;


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            sqlString=" select ST_AsPng(ST_ColorMap(ST_Union(rast),1,'greyscale','EXACT'), ARRAY['ZLEVEL=1']) " +
                    "from "+tname+" " +
                    "where ST_Intersects(ST_Transform(ST_GeomFromText('"+ polygon+"',"+srid+"),"+srid2+"), " +
                    "ST_Polygon(rast))";


            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {


                imgOut = tdb.getPStmt().getResultSet().getBytes(1);


                System.out.println("Image Readed length: "+imgOut.length);

            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));


            return responseBuilder.build();
        }else{
            Response.ResponseBuilder responseBuilder = Response.ok((imgOut));
            responseBuilder.header("Content-Disposition", "attachment; filename=\"MyImageFile.tiff\"");

            return responseBuilder.build();
        }

    }

    @GET
    @Produces("image/tiff")
    @Path("/j_extract_tiff")
    public Response extractRaster(@QueryParam("table_name") String tname,
                                     @QueryParam("polygon") String polygon,
                                     @QueryParam("srid") String srid,
                                     @QueryParam("streamed") String streamed){
        TDBManager tdb=null;
        byte[] imgOut=null;

        try {

            String legend;


            ByteArrayOutputStream is;


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            sqlString=" select ST_AsGDALRaster(ST_Union(rast), 'GTiff') " +
                    "from "+tname+" " +
                    "where ST_Contains(ST_GeomFromText('"+ polygon+"',"+srid+"), " +
                    "ST_Polygon(rast))";



            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {


                imgOut = tdb.getPStmt().getResultSet().getBytes(1);


                System.out.println("Image Readed length: "+imgOut.length);

            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));


            return responseBuilder.build();
        }else{
            Response.ResponseBuilder responseBuilder = Response.ok((imgOut));
            responseBuilder.header("Content-Disposition", "attachment; filename=\"MyImageFile.tiff\"");

            return responseBuilder.build();
        }


    }

    @GET
    @Produces("image/tiff")
    @Path("/j_untiled_tiff")
    public Response getUnTiledRaster(@QueryParam("table_name") String tname,
                                     @QueryParam("ulx") String ulx,
                                     @QueryParam("uly") String uly,
                                     @QueryParam("llx") String llx,
                                     @QueryParam("lly") String lly,
                                     @QueryParam("srid") String srid,
                                     @QueryParam("streamed") String streamed){
        TDBManager tdb=null;
        byte[] imgOut=null;

        try {

            String legend;


            ByteArrayOutputStream is;


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            sqlString=" select ST_AsGDALRaster(ST_Union(rast), 'GTiff') " +
                    "from "+tname+" " +
                    "where ST_Contains(ST_GeomFromText('POLYGON(("+ulx+" "+uly+","+llx+" "+uly+","+llx+" "+lly+","+ulx+" "+lly+","+ulx+" "+uly+"))',"+srid+"), " +
                    "ST_Polygon(rast))";

            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {


                imgOut = tdb.getPStmt().getResultSet().getBytes(1);


                System.out.println("Image Readed length: "+imgOut.length);

            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));


            return responseBuilder.build();
        }else{
            Response.ResponseBuilder responseBuilder = Response.ok((imgOut));
            responseBuilder.header("Content-Disposition", "attachment; filename=\"MyImageFile.tiff\"");

            return responseBuilder.build();
        }
    }

    @GET
    @Produces("image/tiff")
    @Path("/j_merged_tiff")
    public Response getTiledRaster(@QueryParam("table_name") String tname,
                                   @QueryParam("ulx") String ulx,
                                   @QueryParam("uly") String uly,
                                   @QueryParam("llx") String llx,
                                   @QueryParam("lly") String lly,
                                   @QueryParam("streamed") String streamed){
        TDBManager tdb=null;
        byte[] imgOut=null;

        try {

            String legend;


            ByteArrayOutputStream is;


            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            sqlString=" select ST_AsGDALRaster(ST_Union(rast), 'GTiff') " +
                    "from "+tname+" " +
                    "where ST_Contains(ST_GeomFromText('POLYGON(("+ulx+" "+uly+","+llx+" "+uly+","+llx+" "+lly+","+ulx+" "+lly+","+ulx+" "+uly+"))',4326), " +
                    "ST_Polygon(rast))";

            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {


                imgOut = tdb.getPStmt().getResultSet().getBytes(1);


                System.out.println("Image Readed length: "+imgOut.length);

            }

            tdb.closeConnection();
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));


            return responseBuilder.build();
        }else{
            Response.ResponseBuilder responseBuilder = Response.ok((imgOut));
            responseBuilder.header("Content-Disposition", "attachment; filename=\"MyImageFile.tiff\"");

            return responseBuilder.build();
        }
    }


    @GET
    @Produces("image/png")
    @Path("/j_get_whole_png/{image_type}/{year}/{doy}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}{filename:(/filename/.+?)?}")
    public Response extractWholePngPathDOY(@PathParam("image_type") String image_type,
                                    @PathParam("year") String year,
                                    @PathParam("doy") String doy,
                                           @PathParam("polygon") String polygon,
                                           @PathParam("srid_from") String sridfrom,
                                           @PathParam("filename") String filename
                                           ){

        byte[] imgOut=null;
        TDBManager tdb=null;


        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            String reclass_param="", legend_param="", rast_out="";

            if(image_type.matches("tci") ){
                reclass_param = TCI_RECLASS;
                legend_param  = TCI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1," +
                            "ST_Transform(" +
                            "ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";
            }else if(image_type.matches("vci") || image_type.matches("evci") ){
                reclass_param = VCI_RECLASS;
                legend_param  = TCI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1," +
                            "ST_Transform(" +
                            "ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";
            }else if(image_type.matches("spi3") || image_type.matches("spi6") || image_type.matches("spi12") ){
                reclass_param = SPI_RECLASS;
                legend_param  = SPI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";

            }else if(image_type.matches("vhi") || image_type.matches("evhi")){
                reclass_param = VHI_RECLASS;
                legend_param  = VHI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";

            }else{

                legend_param  = "grayscale";
                rast_out = "ST_Union(rast) ";

            }

           
            if(image_type.matches(CRU_IMAGE.toLowerCase())){

   

                sqlString = "select ST_asPNG((ST_ColorMap(postgis.calculate_seasonal_forecast_spi3(" +
                        "ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),"+year+","+doy+",'"+image_type+"'),1,'"+
                        CRUD_LEGEND+"','EXACT'))" +
                        ")";
            }else{
                sqlString="select ST_asPNG(ST_ColorMap("+rast_out+",1,'"+legend_param+"','EXACT'),ARRAY[1,2,3,4], ARRAY['ZLEVEL=3']) " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy+" "+
                        "and   ST_Intersects(rast,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"))";

            }



            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }

        if(filename.matches("") || filename == null){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

            return responseBuilder.build();

        }else{
            Response.ResponseBuilder responseBuilder = Response.ok((imgOut));
            responseBuilder.header("Content-Disposition", "attachment; filename=\""+filename.split("/")[2]+".png\"");

            return responseBuilder.build();
        }

    }


    @GET
    @Produces("image/png")
    @Path("/j_get_whole_png/{image_type}/{year}/{doy}{region_name:(/region_name/.+?)?}{from_srid:(/from_srid/.+?)?}")
    public Response extractWholePngPathDOY2(@PathParam("image_type") String image_type,
                                           @PathParam("year") String year,
                                           @PathParam("doy") String doy,
                                           @PathParam("region_name") String region_name,
                                           @PathParam("from_srid") String from_srid
    ){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();

        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            System.out.println("ci sono ");
            String reclass_param="", legend_param="", rast_out="", the_geom="", polygon_out="";



            if(image_type.matches("tci") || image_type.matches("vci")){
                reclass_param = TCI_RECLASS;
                legend_param  = TCI_LEGEND;

            }else if(image_type.matches("spi3") || image_type.matches("spi6") || image_type.matches("spi12") ){
                reclass_param = SPI_RECLASS;
                legend_param  = SPI_LEGEND;
            //    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+the_geom+"',"+from_srid.split("/")[2]+"),"+DBSRID+"),true),1,'"+reclass_param+"','8BUI') ";

            }else if(image_type.matches("vhi")){
                reclass_param = VHI_RECLASS;
                legend_param  = VHI_LEGEND;
          //      rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+the_geom+"',"+from_srid.split("/")[2]+"),"+DBSRID+"),true),1,'"+reclass_param+"','8BUI') ";

            }else{

                legend_param  = "grayscale";
            //    rast_out = "ST_Union(rast) ";

            }

            rast_out = "(select ST_Union(rast) from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                    "where extract('year' from b.dtime) = "+year+" "+
                    "and   extract('doy' from b.dtime) = "+doy + ")";

            polygon_out = "(select ST_Union(the_geom) from postgis.region_geoms " +
                    "inner join postgis.regions using (_id_region) where name = '"+region_name.split("/")[2]+"')";

            rast_out = "ST_Reclass("+rast_out+",1,'"+reclass_param+"', '8BUI')";

            rast_out = "ST_Clip("+rast_out+",1," + polygon_out +
                    ",false) ";



            sqlString="select ST_asPNG(ST_ColorMap("+rast_out+",1,'"+legend_param+"','EXACT')) ";


            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

        return responseBuilder.build();

    }


    @GET
    @Produces("image/png")
    @Path("/j_get_whole_png/{image_type}/{year}/{month}/{day}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response extractWholePngPathDMY(@PathParam("image_type") String image_type,
                                    @PathParam("year") String year,
                                    @PathParam("month") String month,
                                    @PathParam("day") String day,
                                           @PathParam("polygon") String polygon,
                                           @PathParam("srid_from") String sridfrom){

        byte[] imgOut=null;
        TDBManager tdb=null;



        try {

            if (day == null)
                day = "1";

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            String reclass_param="", legend_param="", rast_out="";


            if(image_type.matches("tci") ){
                reclass_param = TCI_RECLASS;
                legend_param  = TCI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1," +
                            "ST_Transform(" +
                            "ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";
            }else if(image_type.matches("vci") || image_type.matches("evci") ){
                reclass_param = VCI_RECLASS;
                legend_param  = TCI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1," +
                            "ST_Transform(" +
                            "ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),-999.0,true),1,'"+reclass_param+"','8BUI',-999.0) ";
            }else if(image_type.matches("spi3") || image_type.matches("spi6") || image_type.matches("spi12") ){
                reclass_param = SPI_RECLASS;
                legend_param  = SPI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),1,'"+reclass_param+"','8BUI') ";

            }else if(image_type.matches("vhi")|| image_type.matches("evhi")){
                reclass_param = VHI_RECLASS;
                legend_param  = VHI_LEGEND;
                if(polygon.matches("") || polygon == null)
                    rast_out = "ST_Reclass(ST_Union(rast),1,'"+reclass_param+"','8BUI') ";
                else
                    rast_out = "ST_Reclass(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),1,'"+reclass_param+"','8BUI') ";

            }else{

                legend_param  = "grayscale";
                rast_out = "ST_Union(rast) ";

            }


            sqlString="select ST_asPNG(ST_ColorMap("+rast_out+",1,'"+legend_param+"','EXACT')) " +
                    "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                    "where extract('year' from b.dtime) = "+year+" "+
                    "and   extract('month' from b.dtime) = "+month+" "+
                    "and   extract('day' from b.dtime) = "+day;

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+day+"-"+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }



        Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

        return responseBuilder.build();

    }


    /**
     * Get image as AAIGrid
     * @param image_type
     * @param year
     * @param doy
     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/j_get_whole_aaigrid/{image_type}/{year}/{doy}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response extractWholeAAIGridPathDOY(@PathParam("image_type") String image_type,
                                           @PathParam("doy") String doy,
                                               @PathParam("year") String year,
                                               @PathParam("polygon") String polygon,
                                               @PathParam("srid_from") String sridfrom){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();

        try {



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            if(polygon.matches("") || polygon == null){
                sqlString="select ST_asGDALRaster(ST_Union(rast),'AAIGrid') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy;
            }else{
                sqlString="select ST_asGDALRaster(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),'AAIGrid') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy;
            }



            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }



        Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

        return responseBuilder.build();

    }

    /**
     * Get image as AAIGrid
     * @param image_type
     * @param year
     * @param month
     * @param day
     * @return
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/j_get_whole_aaigrid/{image_type}/{year}/{month}/{day}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response extractWholeAAIGridPathDMY(@PathParam("image_type") String image_type,
                                               @PathParam("year") String year,
                                               @PathParam("month") String month,
                                               @PathParam("day") String day,
                                               @PathParam("polygon") String polygon,
                                               @PathParam("srid_from") String sridfrom){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();

        try {

            if (day == null)
                day = "1";

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;
            if(polygon.matches("") || polygon == null){
                sqlString="select ST_asGDALRaster(ST_Union(rast),'AAIGrid') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('month' from b.dtime) = "+month+" "+
                        "and   extract('day' from b.dtime) = "+day;
            }else{
                sqlString="select ST_asGDALRaster(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),'AAIGrid') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('month' from b.dtime) = "+month+" "+
                        "and   extract('day' from b.dtime) = "+day;
            }



            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+day+"-"+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }

        Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

        return responseBuilder.build();
    }

    @GET
    @Produces("image/png")
    @Path("/j_get_whole_png")
    public Response extractWholePng(@QueryParam("image_type") String image_type,
                                       @QueryParam("year") String year,
                                       @QueryParam("month") String month,
                                       @QueryParam("day") String day,
                                       @QueryParam("doy") String doy,
                                       @QueryParam("streamed") String streamed){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();
        GregorianCalendar gc = new GregorianCalendar();

        try {
            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.HOUR_OF_DAY,0);
            gc.set(Calendar.MINUTE,0);
            gc.set(Calendar.SECOND,0);
            gc.set(Calendar.MILLISECOND,0);

            if (doy == null){
                if (day == null)
                    day = "1";

                System.out.println("Mese e Giorno : "+ month + " "+day);
                gc.set(Calendar.MONTH, (Integer.parseInt(month)-1));
                gc.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
            }else{

                System.out.println("GG : "+doy);
                gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));
            }




            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select ST_asPNG(ST_ColorMap(ST_Union(rast),1,'grayscale')) " +
                    "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                    "where dtime = ?";

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+day+"-"+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

            return responseBuilder.build();
        }else{

            Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
            responseBuilder.header("Content-Disposition", "attachment; filename=\""+image_type+"_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".png\"");

            return responseBuilder.build();
        }
    }

    @GET
    @Produces("image/gtiff")
    @Path("/j_get_whole_gtiff/{image_type}/{year}/{doy}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response extractWholeTiffDOY(@PathParam("image_type") String image_type,
                                     @PathParam("year") String year,
                                     @PathParam("doy") String doy,
                                        @PathParam("polygon") String polygon,
                                        @PathParam("srid_from") String sridfrom){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();

        try {



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            if(polygon.matches("") || polygon == null){
                sqlString="select ST_asGDALRaster((select ST_Union(rast) " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy +" "+
                        "),'GTiff')" ;
            }else{

                sqlString="select ST_asGDALRaster(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),'GTiff') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy;

            }



            System.out.println("SQL : "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }



        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\""+image_type+"_"+year+"_"+doy+".tiff\"");
        return responseBuilder.build();
    }

    @GET
    @Produces("image/gtiff")
    @Path("/j_get_whole_gtiff/{image_type}/{year}/{month}/{day}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response extractWholeTiffDMY(@PathParam("image_type") String image_type,
                                     @PathParam("year") String year,
                                     @PathParam("month") String month,
                                     @PathParam("day") String day,
                                        @PathParam("polygon") String polygon,
                                        @PathParam("srid_from") String sridfrom){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();

        try {

            if (day == null)
                day = "1";

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            if(polygon.matches("") || polygon == null){
                sqlString="select ST_asGDALRaster(ST_Union(rast),'GTiff') " +
                        "from postgis."+image_type+" inner join postgis.acquisizioni using (id_acquisizione) "+
                        "where extract('year' from dtime) = "+year+" "+
                        "and   extract('month' from dtime) = "+month+" "+
                        "and   extract('day' from dtime) = "+day;
            }else{
                sqlString="select ST_asGDALRaster(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),'GTiff') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('month' from b.dtime) = "+month+" "+
                        "and   extract('day' from b.time) = "+day;
            }


            System.out.println("SQL : "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+day+"-"+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }




        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\""+image_type+"_"+year+"_"+month+"_"+day+".tiff\"");

        return responseBuilder.build();

    }



    @GET
    @Produces("image/gtiff")
    @Path("/j_get_whole_gtiff")
    public Response extractWholeTiff(@QueryParam("image_type") String image_type,
                                       @QueryParam("year") String year,
                                       @QueryParam("month") String month,
                                       @QueryParam("day") String day,
                                       @QueryParam("doy") String doy,
                                       @QueryParam("streamed") String streamed){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {



            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.HOUR_OF_DAY,0);
            gc.set(Calendar.MINUTE,0);
            gc.set(Calendar.SECOND,0);
            gc.set(Calendar.MILLISECOND,0);

            if (doy == null){
                if (day == null)
                    day = "1";

                System.out.println("Mese e Giorno : "+ month + " "+day);
                gc.set(Calendar.MONTH, (Integer.parseInt(month)-1));
                gc.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
            }else{

                System.out.println("GG : "+doy);
                gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));
            }



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select ST_asGDALRaster(ST_Union(rast),'GTiff') " +
                    "from postgis."+image_type+" inner join postgis.acquisizioni using (id_acquisizione) "+
                    "where dtime=? ";

            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+day+"-"+month+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

            return responseBuilder.build();
        }else{

            Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
            responseBuilder.header("Content-Disposition", "attachment; filename=\""+image_type+"_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".tiff\"");

            return responseBuilder.build();
        }
    }

    /**
     *
     * Calculate TCI image related to specific date composed by year and day of year otherwise it is possible pass year, month and day.
     * this method checks if any TCI exists and, in this case, it will extract it from DB directly.
     *
     * @param year   (Mandatory)
     * @param month  (optional with day)
     * @param day    (optional with month)
     * @param gg     (optional without day and month)
     * @param format (Mandatory GTIFF, PNG, AAIGrid)
     * @param store  (Optional store result in geodb)
     * @param streamed
     * @param colormap
     * @param force   (Optional force calculating and storing)
     * @param  normalize (Optional true : perform image reclassification between 0-100 values. Default is true)
     * @return Response with image (or error message)
     */

    //TODO: aggiungere parametro per calcolo in background senza rilascio di immagini (pensare ad overload di func)
    @GET
    @Produces("image/png")
    @Path("/j_calc_tci")
    public Response calculateTCI(@QueryParam("year") String year,
                                    @QueryParam("month") String month,
                                    @QueryParam("day") String day,
                                    @QueryParam("gg") String gg,
                                    @QueryParam("format") String format,
                                    @QueryParam("store") String store,
                                    @QueryParam("streamed") String streamed,
                                    @QueryParam("colormap") String colormap,
                                    @QueryParam("force") String force,
                                    @QueryParam("normalize") String normalize){

        byte[] imgOut=null;


        boolean create_it;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();
        String normalize2;
        try {

            if(store == null){
                store = "true";
            }

            if(force == null){
                force = "false";
            }

            if(normalize == null){
                normalize = "st_reclass(rast,1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
                normalize2 = "st_reclass(postgis.calculate_tci(?,"+store+"),1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
            }else if(normalize.matches("true")){
                normalize = "st_reclass(rast,1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
                normalize2 = "st_reclass(postgis.calculate_tci(?,"+store+"),1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
            }else{
                normalize = "rast";
                normalize2 = "postgis.calculate_tci(?,"+store+")";
            }

            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.HOUR_OF_DAY,0);
            gc.set(Calendar.MINUTE,0);
            gc.set(Calendar.SECOND,0);
            gc.set(Calendar.MILLISECOND,0);



            if (gg == null){
                System.out.println("Mese e Giorno : "+ month + " "+day);
                gc.set(Calendar.MONTH, (Integer.parseInt(month)-1));
                gc.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
            }else{

                System.out.println("GG : "+gg);
                gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(gg));
            }



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            //Check if TCI exists
            if(format.matches(PNG)){
                System.out.println("PNG selected");



                sqlString="select ST_asPNG(ST_ColorMap("+normalize+",1,'"+TCI_LEGEND+"','EXACT')), a.id_acquisizione " +
                        "from postgis.tci as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where b.dtime = ?";
            }else if(format.matches(GTIFF)){
                System.out.println("GTIFF selected");

                sqlString="select ST_asGDALRaster("+normalize+",'GTiff'), a.id_acquisizione " +
                        "from postgis.tci as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where b.dtime = ?";
            }else if(format.matches(AAIGrid)){
                System.out.println("AAIGrid selected");

                sqlString="select ST_asGDALRaster("+normalize+",'AAIGrid'), a.id_acquisizione " +
                        "from postgis.tci as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where b.dtime = ?";
            }

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.print("TCI exists...");

                if(force.matches("true")){


                    System.out.println("it will be recreated");

                    String id_acquisizione = ""+tdb.getInteger(2);
                    sqlString="delete from postgis.tci where id_acquisizione = "+id_acquisizione;
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();

                    System.out.print("old image deleted...");


                    sqlString="delete from postgis.acquisizioni where id_acquisizione = "+id_acquisizione;
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();
                    System.out.println("old acquisizione deleted");
                    create_it=true;
                }else{
                    create_it=false;

                    System.out.println("it will be used as output");
                    imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                    System.out.println("Image Readed length: "+imgOut.length);
                }
            }else{
                System.out.println("TCI does not exist, it will be calculated");

                create_it=true;
            }


            if(create_it){
                if(format.matches(PNG)){

                    sqlString="select ST_asPNG(ST_ColorMap("+normalize2+",1,'"+TCI_LEGEND+"','EXACT')) ";
                }else if(format.matches(GTIFF)){

                    sqlString="select ST_asGDALRaster("+normalize2+",'GTiff') ";
                }else if(format.matches(AAIGrid)){

                    sqlString="select ST_asGDALRaster("+normalize2+",'AAIGrid') ";
                }


                tdb.setPreparedStatementRef(sqlString);

                tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

                tdb.runPreparedQuery();

                if (tdb.next()) {
                    imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                    System.out.println("Image Readed length: "+imgOut.length);
                }else{
                    try{
                        tdb.closeConnection();
                    }catch (Exception ee){
                        System.out.println("Error "+ee.getMessage());
                    }
                    return  Response.status(Response.Status.OK).entity("Error occurred: maybe the TCI image of "+day+"-"+month+"-"+year+" doesn't exist ").build();
                }
            }
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

            return responseBuilder.build();
        }else{

            Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
            if(format.matches(PNG)){
                responseBuilder.header("Content-Disposition", "attachment; filename=\"TCI_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".png\"");
            }else if(format.matches(GTIFF)){
                responseBuilder.header("Content-Disposition", "attachment; filename=\"TCI_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".tiff\"");
            }else if(format.matches(AAIGrid)){
                responseBuilder.header("Content-Disposition", "attachment; filename=\"TCI_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".txt\"");
            }


            return responseBuilder.build();
        }
    }


    /**
     *
     * Calculate VCI image related to specific date composed by year and day of year otherwise it is possible pass year, month and day.
     * this method checks if any VCI exists and, in this case, it will extract it from DB directly.
     *
     * @param year   (Mandatory)
     * @param month  (optional with day)
     * @param day    (optional with month)
     * @param gg     (optional without day and month)
     * @param format (Mandatory GTIFF, PNG, AAIGrid)
     * @param store  (Optional store result in geodb)
     * @param streamed
     * @param  normalize (Optional true : perform image reclassification between 0-100 values. Default is true)
     * @return Response with image (or error message)
     */
    @GET
    @Produces("image/png")
    @Path("/j_calc_vci")
    public Response calculateVCI(@QueryParam("year") String year,
                                 @QueryParam("month") String month,
                                 @QueryParam("day") String day,
                                 @QueryParam("gg") String gg,
                                 @QueryParam("format") String format,
                                 @QueryParam("store") String store,
                                 @QueryParam("streamed") String streamed,
                                 @QueryParam("imgtype") String imgtype,
                                 @QueryParam("normalize") String normalize,
                                 @QueryParam("polygon") String polygon,
                                 @QueryParam("srid") String srid
                                 ){

        byte[] imgOut=null;


        boolean create_it;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();
        String normalize2;
        try {

            if(store == null){
                store = "true";
            }

  //          if(force == null){
  //              force = "false";
  //          }

            if(normalize == null){
                normalize = "st_reclass(rast,1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
                normalize2 = "st_reclass(postgis.calculate_vci(?,ST_GeomFromText('"+polygon+"',"+srid+"),"+store+"),1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
            }else if(normalize.matches("true")){
                normalize = "st_reclass(rast,1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
                normalize2 = "st_reclass(postgis.calculate_vci(?,ST_GeomFromText('"+polygon+"',"+srid+"),"+store+"),1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
            }else{
                normalize = "rast";
                normalize2 = "postgis.calculate_vci(?,ST_GeomFromText('"+polygon+"',"+srid+"),"+store+")";
            }

            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.HOUR_OF_DAY,0);
            gc.set(Calendar.MINUTE,0);
            gc.set(Calendar.SECOND,0);
            gc.set(Calendar.MILLISECOND,0);



            if (gg == null){
                System.out.println("Mese e Giorno : "+ month + " "+day);
                gc.set(Calendar.MONTH, (Integer.parseInt(month)-1));
                gc.set(Calendar.DAY_OF_MONTH, Integer.parseInt(day));
            }else{

                System.out.println("GG : "+gg);
                gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(gg));
            }



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            //Check if TCI exists
  /*          if(format.matches(PNG)){
                System.out.println("PNG selected");



                sqlString="select ST_asPNG(ST_ColorMap("+normalize+",1,'"+VCI_LEGEND+"','EXACT')), a.id_acquisizione " +
                        "from postgis.vci as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where b.dtime = ?";
            }else if(format.matches(GTIFF)){
                System.out.println("GTIFF selected");

                sqlString="select ST_asGDALRaster("+normalize+",'GTiff'), a.id_acquisizione " +
                        "from postgis.vci as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where b.dtime = ?";

                System.out.println("SQL: "+sqlString);
            }else if(format.matches(AAIGrid)){
                System.out.println("AAIGrid selected");

                sqlString="select ST_asGDALRaster("+normalize+",'AAIGrid'), a.id_acquisizione " +
                        "from postgis.vci as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where b.dtime = ?";
            }

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.print("VCI exists...");

                if(force.matches("true")){


                    System.out.println("it will be recreated");

                    String id_acquisizione = ""+tdb.getInteger(2);
                    sqlString="delete from postgis.vci where id_acquisizione = "+id_acquisizione;
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();

                    System.out.print("old image deleted...");


                    sqlString="delete from postgis.acquisizioni where id_acquisizione = "+id_acquisizione;
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.performInsert();
                    System.out.println("old acquisizione deleted");
                    create_it=true;
                }else{
                    create_it=false;

                    System.out.println("it will be used as output");
                    imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                    System.out.println("Image Readed length: "+imgOut.length);
                }
            }else{
                System.out.println("VCI does not exist, it will be calculated");

                create_it=true;
            }
*/

 //           if(create_it){
                if(format.matches(PNG)){

                    sqlString="select ST_asPNG(ST_ColorMap("+normalize2+",1,'"+TCI_LEGEND+"','EXACT')) ";
                }else if(format.matches(GTIFF)){

                    sqlString="select ST_asGDALRaster("+normalize2+",'GTiff') ";

                    System.out.println("SQL: "+sqlString);
                }else if(format.matches(AAIGrid)){

                    sqlString="select ST_asGDALRaster("+normalize2+",'AAIGrid') ";
                }


                tdb.setPreparedStatementRef(sqlString);

                tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

                tdb.runPreparedQuery();

                if (tdb.next()) {
                    imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                    System.out.println("Image Readed length: "+imgOut.length);
                }else{
                    try{
                        tdb.closeConnection();
                    }catch (Exception ee){
                        System.out.println("Error "+ee.getMessage());
                    }
                    return  Response.status(Response.Status.OK).entity("Error occurred: maybe the VCI image of "+day+"-"+month+"-"+year+" doesn't exist ").build();
                }
 //           }
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }


        if(streamed.matches("1")){
            Response.ResponseBuilder responseBuilder = Response.ok(new ByteArrayInputStream(imgOut));

            return responseBuilder.build();
        }else{

            Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
            if(format.matches(PNG)){
                responseBuilder.header("Content-Disposition", "attachment; filename=\"VCI_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".png\"");
            }else if(format.matches(GTIFF)){
                responseBuilder.header("Content-Disposition", "attachment; filename=\"VCI_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".tiff\"");
            }else if(format.matches(AAIGrid)){
                responseBuilder.header("Content-Disposition", "attachment; filename=\"VCI_"+gc.get(Calendar.YEAR)+"_"+gc.get(Calendar.DAY_OF_YEAR)+".txt\"");
            }


            return responseBuilder.build();
        }
    }

    /**
     * Service for Image extraction.
     * @param image_type  - Dataset to be used for extraction
     * @param year		  - year
     * @param doy		  - doy
     * @param outformat   - output format (png, gtiff, aaigrid)
     * ----------------OPTIONS---------------------------------
     * @param polygon     - polygon for extraction under specified area
     * @param sridfrom	  - SRID of given polygon
     * @param factor	  - factor for image downscaling
     * @return
     */
    @GET
    @Produces("image/gtiff")
    @Path("/j_get_image/{image_type}/{year}/{doy}/{outformat}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}{factor:(/factor/.+?)?}")
    public Response extractImageDOY(@PathParam("image_type") String image_type,
                                        @PathParam("year") String year,
                                        @PathParam("doy") String doy,
                                        @PathParam("outformat") String outformat,
                                        @PathParam("polygon") String polygon,
                                        @PathParam("srid_from") String sridfrom,
                                        @PathParam("factor") String factor){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();
        String downscaling_prefix="";
        try {



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            //check factor for downscaling
            if(!(factor.matches("") || factor == null)) {
            	downscaling_prefix = "o_"+factor.split("/")[2]+"_";            	
            }
            
            if(polygon.matches("") || polygon == null){

            	sqlString="select ST_asGDALRaster((select ST_Union(rast) " +
                          "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                          "where extract('year' from b.dtime) = "+year+" "+
                          "and   extract('doy' from b.dtime) = "+doy +" "+
                          "),'"+outformat+"')" ;

            

            }else{

                sqlString="select ST_asGDALRaster(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),'"+outformat+"') " +
                        "from postgis."+downscaling_prefix+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy+" " +
                        "and   ST_Intersects(rast,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"))";
                ;

            }



            System.out.println("SQL : "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }



        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\""+image_type+"_"+year+"_"+doy+".tiff\"");
        return responseBuilder.build();
    }


    @GET
    @Produces("text/xml")
    @Path("/j_get_wms/{image_type}/{year}/{doy}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}{region:(/region/.+?)?}")
    public Response getImageWMS(@PathParam("image_type") String image_type,
                                        @PathParam("year") String year,
                                        @PathParam("doy") String doy,
                                        @PathParam("polygon") String polygon,
                                        @PathParam("srid_from") String sridfrom){

        String imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();

        try {



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            if(polygon.matches("") || polygon == null){

             
	            sqlString="select ST_asGDALRaster((select ST_Union(rast) " +
	                    "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
	                    "where extract('year' from b.dtime) = "+year+" "+
	                    "and   extract('doy' from b.dtime) = "+doy +" "+
	                    "),'WMS')" ;


            }else{

                sqlString="select ST_asGDALRaster(ST_Clip(ST_Union(rast),1,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"),true),'WMS') " +
                        "from postgis."+image_type+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                        "where extract('year' from b.dtime) = "+year+" "+
                        "and   extract('doy' from b.dtime) = "+doy+" " +
                        "and   ST_Intersects(rast,ST_Transform(ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"),"+DBSRID+"))";;

            }



            System.out.println("SQL : "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getString(1);
                System.out.println("Image Readed length: "+imgOut);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image "+image_type+" of "+doy+"-"+year+" not found ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {
            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
        }



        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
       // responseBuilder.header("Content-Disposition", "attachment; filename=\""+image_type+"_"+year+"_"+doy+".tiff\"");
        return responseBuilder.build();
    }

}
