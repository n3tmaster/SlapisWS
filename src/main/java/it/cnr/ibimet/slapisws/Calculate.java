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
import java.text.DecimalFormat;
import java.util.*;

/**
 * Created by lerocchi on 16/02/17.
 *
 * GetRaster
 *
 * retrieves raster data from postgis
 */
@Path("/calculate")

/**
 * Created by lerocchi on 14/09/17.
 */
public class Calculate  extends Application implements SWH4EConst{


    @GET
    @Produces("image/tiff")
    @Path("/rain_cum/{year}/{month}")
    public Response calculateRainSum(@PathParam("year") String year,
                                 @PathParam("month") String month){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select ST_asGDALRaster(postgis.calculate_rain_cum("+month+","+year+"),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image rain sum of "+month+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"raincum_"+year+"_"+month+".tiff\"");

        return responseBuilder.build();


    }


    @GET
    @Produces("image/tiff")
    @Path("/lst_max/{year}/{doy}")
    public Response calculateLSTMax(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));



            sqlString="select ST_asGDALRaster(postgis.calculate_lst_max(?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"lst_max_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }


    @GET
    @Produces("image/tiff")
    @Path("/lst_min/{year}/{doy}")
    public Response calculateLSTMin(@PathParam("year") String year,
                                    @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));



            sqlString="select ST_asGDALRaster(postgis.calculate_lst_min(?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"lst_min_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }



    @GET
    @Produces("image/tiff")
    @Path("/resample_tci/{year}/{doy}")
    public Response calculateResampleTCI(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            sqlString="select ST_asGDALRaster(postgis.calculate_resample_tci("+doy+", "+year+"),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"resample_tci_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }




    @GET
    @Produces("image/tiff")
    @Path("/mean_tci/{year}/{doy}")
    public Response calculateMeanTCI(@PathParam("year") String year,
                                    @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;

        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;


            sqlString="select ST_asGDALRaster(postgis.calculate_mean_tci("+doy+", "+year+"),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"mean_tci_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }



    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/{imgtype}/{year}/{doy}/{normalized}")
    public Response calculateTCI(@PathParam("imgtype") String imgtype,
                                 @PathParam("year") String year,
                                 @PathParam("doy") String doy,
                                 @PathParam("normalized") String normalized){

        byte[] imgOut=null;


        boolean create_it;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();
        String normalize2;
        try {



            if(normalized.toLowerCase().matches(NORMALIZED)){
            //    normalize = "st_reclass(rast,1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
                normalize2 = "st_reclass(postgis.calculate_"+imgtype+"(?,true),1,'[0.0-100.0]:1-100,(100.0-32767.0]:100','8BUI')";
            }else if(normalized.toLowerCase().matches(REAL)){

            //    normalize = "rast";
                normalize2 = "postgis.calculate_"+imgtype+"(?,true)";
            }else{

                //Error
                normalize2="";
                return Response.status(500).entity("Missing NORMALIZED parameter!").build();

            }

            gc.set(Calendar.YEAR, Integer.parseInt(year));
            gc.set(Calendar.HOUR_OF_DAY,0);
            gc.set(Calendar.MINUTE,0);
            gc.set(Calendar.SECOND,0);
            gc.set(Calendar.MILLISECOND,0);
            gc.set(Calendar.DAY_OF_YEAR, Integer.parseInt(doy));



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            //Check if TCI exists
            sqlString="select rast, a.id_acquisizione " +
                    "from postgis."+imgtype+" as a inner join postgis.acquisizioni as b using (id_acquisizione) "+
                    "where b.dtime = ?";


            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

            tdb.runPreparedQuery();

            if (tdb.next()) {

                System.out.println(imgtype.toUpperCase()+" exists...it will be recreated");


                String id_acquisizione = ""+tdb.getInteger(2);
                sqlString="delete from postgis."+imgtype+" where id_acquisizione = "+id_acquisizione;
                tdb.setPreparedStatementRef(sqlString);
                tdb.performInsert();

                System.out.print("old image deleted...");


                sqlString="delete from postgis.acquisizioni where id_acquisizione = "+id_acquisizione;
                tdb.setPreparedStatementRef(sqlString);
                tdb.performInsert();
                System.out.println("old acquisizione deleted");
                create_it=true;

            }



            sqlString="select "+normalize2;

            System.out.println(sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.DATE,gc,1);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.OK).entity(imgtype.toUpperCase()+" image of "+doy+"-"+year+" calculated ").build();
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.OK).entity("Error occurred: maybe the "+imgtype.toUpperCase()+" image of "+doy+"-"+year+" doesn't exist ").build();
            }

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }



    }



    @GET
    @Produces("image/tiff")
    @Path("/ndvi_max/{year}/{doy}")
    public Response calculateNDVIMax(@PathParam("year") String year,
                                    @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;




            sqlString="select ST_asGDALRaster(postgis.calculate_ndvi_max(?,?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.INT,doy,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"ndvi_max_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }

    @GET
    @Produces("image/tiff")
    @Path("/ndvi_min/{year}/{doy}")
    public Response calculateNDVIMin(@PathParam("year") String year,
                                     @PathParam("doy") String doy){

        byte[] imgOut=null;
        TDBManager tdb=null;
        GregorianCalendar gc = new GregorianCalendar();
        Vector<InputStream> inputStreams = new Vector<InputStream>();




        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;




            sqlString="select ST_asGDALRaster(postgis.calculate_ndvi_min(?,?),'GTiff') ";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.setParameter(DBManager.ParameterType.INT,doy,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("eccomi");
                imgOut = tdb.getPStmt().getResultSet().getBytes(1);
                System.out.println("Image Readed length: "+imgOut.length);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Image lst of "+doy+"-"+year+" not found ").build();
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


        System.out.println("fine");

        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);
        responseBuilder.header("Content-Disposition", "attachment; filename=\"ndvi_min_"+year+"_"+doy+".tiff\"");

        return responseBuilder.build();


    }


    @GET
    @Produces("text/plain")
    @Path("/spi/{step}")
    public Response calculateNDVIMin(@PathParam("step") String step){

        TDBManager tdb=null;


        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;




            System.out.println("Run SPI calculus");
            sqlString="INSERT INTO postgis.spitemp (dmonth, dyear, drast) " +
                    "SELECT * FROM postgis.spi_on_matrix("+step+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);


            tdb.runPreparedQuery();

            if (tdb.next()) {
                System.out.println("Converting R dataframe into Postgis RASTER");
                sqlString="SELECT postgis.save_spi_data("+step+")";
                System.out.println("SQL: "+sqlString);

                tdb.setPreparedStatementRef(sqlString);
                if (tdb.next()) {

                }else{
                    try{
                        tdb.closeConnection();
                    }catch (Exception ee){
                        System.out.println("Error "+ee.getMessage());
                    }
                    return  Response.status(Response.Status.NOT_FOUND).entity("Procedure error N1").build();
                }

            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Procedure error N2").build();
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


        System.out.println("fine");


        return  Response.status(Response.Status.OK).entity("Done").build();


    }


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/series/{image_type}{polygon:(/polygon/.+?)?}{srid_from:(/srid_from/.+?)?}")
    public Response calculateSeries(@PathParam("image_type") String image_type,
                                    @PathParam("polygon") String polygon,
                                    @PathParam("srid_from") String sridfrom){

        byte[] imgOut=null;
        TDBManager tdb=null;
        DecimalFormat df = new DecimalFormat("###.###");
        
        String retData="";
        try {



            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            if(polygon.matches("") || polygon == null || sridfrom.matches("") || sridfrom == null) {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }
                return  Response.status(Response.Status.NOT_FOUND).entity("Polygon is empty").build();
            }else{


                sqlString = "select * from postgis.calculate_stat_series('"+
                        image_type+"', ST_GeomFromText('"+polygon.split("/")[2]+"',"+sridfrom.split("/")[2]+"))";
                tdb.setPreparedStatementRef(sqlString);

                System.out.println(sqlString);

                tdb.runPreparedQuery();
                JSONArray jArray= new JSONArray();
                while (tdb.next()) {

                    JSONObject jobj = new JSONObject();


                    jobj.put("doy", tdb.getInteger(1));
                    jobj.put("year", tdb.getInteger(2));
                    jobj.put("count", tdb.getInteger(7));
                    jobj.put("mean", df.format(tdb.getDouble(3)));
                    jobj.put("stddev", df.format(tdb.getDouble(4)));
                    jobj.put("min", df.format(tdb.getDouble(5)));
                    jobj.put("max", df.format(tdb.getDouble(6)));
                    jobj.put("q25", df.format(tdb.getDouble(8)));
                    jobj.put("q75", df.format(tdb.getDouble(9)));

                    jArray.add(jobj);

                }
                retData = jArray.toJSONString();

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



        Response.ResponseBuilder responseBuilder = Response.ok(retData);

        return responseBuilder.build();
    }

}
