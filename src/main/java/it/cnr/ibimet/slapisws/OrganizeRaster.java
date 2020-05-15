package it.cnr.ibimet.slapisws;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;

import java.nio.file.Paths;
import java.nio.file.Files;


import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;




import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;
import it.cnr.ibimet.restutil.HttpURLManager;
import it.lr.libs.DBManager;
import it.lr.libs.DBManager.ParameterType;


/**
 * Created by lerocchi on 03/07/17.
 *
 * Web Services for organizing new data into GeoDB
 * its methods are called when new image are imported into GeoDB in order to create all metadata and logic link between tables
 */
@Path("/organize")
public class OrganizeRaster extends Application implements SWH4EConst {


    public static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    
    @POST
    @Path("/j_raster_save")
    public Response rasterSave(@FormDataParam("table_name") String tname,
                               @FormDataParam("table_temp") String ttemp,
                               @FormDataParam("year") String year,
                               @FormDataParam("dayofyear") String doy){
        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select from postgis.import_lst_images("+year+","+year+","+doy+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                //new image saved. now deleting temporary table...
                System.out.println("new image saved. now deleting temporary table...");

                sqlString=" select from postgis.clean_temp_lst_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if (tdb.next()){
                    System.out.println("ok");
                }else{


                    return Response.status(500).entity("Error during import procedure!").build();

                }
            }else{
                sqlString=" select from postgis.clean_temp_lst_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();
                return Response.status(500).entity("Error during import procedure!").build();

            }


        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{

                tdb.setPreparedStatementRef(" select from postgis.clean_temp_lst_tables("+year+","+year+","+doy+","+doy+")");

                tdb.runPreparedQuery();
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();


    }


    @POST
    @Path("/j_organize_raster/{table_name}/{year}/{doy}")
    public Response extractWholeTiffDMY(@PathParam("table_name") String table_name,
                                        @PathParam("year") String year,
                                        @PathParam("doy") String doy){

        TDBManager tdb=null;



        try {
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;



            //Save new image into related spatial table and put new timestamp in acquisition table

            sqlString=" select from postgis.import_"+table_name+"_images("+year+","+year+","+doy+","+doy+")";

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if (tdb.next()) {
                //new image saved. now deleting temporary table...
                System.out.println("new image saved. now deleting temporary table...");

                sqlString=" select from postgis.clean_temp_"+table_name+"_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();

                if (tdb.next()){
                    System.out.println("ok");
                }else{


                    return Response.status(500).entity("Error during import procedure!").build();

                }
            }else{
                sqlString=" select from postgis.clean_temp_"+table_name+"_tables("+year+","+year+","+doy+","+doy+")";

                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();
                return Response.status(500).entity("Error during import procedure!").build();

            }


        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{

                tdb.setPreparedStatementRef(" select from postgis.clean_temp_"+table_name+"_tables("+year+","+year+","+doy+","+doy+")");

                tdb.runPreparedQuery();
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error "+ee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            {
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                }

            }
        }


        return Response.status(200).entity("Image saved!").build();

    }


    @POST
    @Path("/j_update_spi/{step}/{srid}/{minx}/{miny}/{maxx}/{maxy}")
    public Response updateSPI(@PathParam("step") String step,
                              @PathParam("minx") String minx,
                              @PathParam("miny") String miny,
                              @PathParam("maxx") String maxx,
                              @PathParam("maxy") String maxy,
                              @PathParam("srid") String srid){

        TDBManager tdb=null;
        int width, height;
        int width_parc, height_parc;
        int imgt=7;
        int i=1;
        int n_threads=-1;
        int id_is=-1;
        String polygon = "POLYGON(("+
                        minx+" "+miny+","+
                        minx+" "+maxy+","+
                        maxx+" "+maxy+","+
                        maxx+" "+miny+","+
                        minx+" "+miny+"))";

        Long resultFuture = new Long(0);

        System.out.println("Polygon: "+polygon);
        //TODO: da migliorare
  //      FutureTask futureTask_1,futureTask_2,futureTask_3,futureTask_4,futureTask_5,futureTask_6,futureTask_7,futureTask_8,futureTask_9,futureTask_10;

        try {

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("SPI calculation, starting time: "+timestamp);

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            if(step.matches("3")){
                imgt=7;
            }else if(step.matches("6")){
                imgt=8;
            }else if(step.matches("12")){
                imgt=9;
            }

            //get the number of tiles and related threads
            sqlString = "select count(*), id_acquisizione " +
                    "   from   postgis.spi" + step + " " +
                    "   where  id_acquisizione = ( select min(id_acquisizione) from postgis.acquisizioni " +
                    "                              where id_imgtype = "+imgt+" ) "+
                    "   and    ST_Intersects(rast, ST_GeomFromText('"+ polygon + "',"+srid+")) "+
                    "   group by 2";

      //      System.out.println("SQL: "+ sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if(tdb.next()){
                n_threads = tdb.getInteger(1);
                id_is = tdb.getInteger(2);
                System.out.println("n_threads: "+n_threads+ " ids: "+id_is);
            }else{

                System.out.println("Nothing was found");

                throw new Exception();
            }
            //get overall extent of dataset and ul coordinates



            sqlString=" select ST_Width(rast), ST_Height(rast), ST_UpperLeftX(rast), ST_UpperLeftY(rast) " +
                    "   from   postgis.spi" + step +
                    "   where  id_acquisizione = "+id_is;


          //  System.out.println("Get overall extent - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            //Launch parallel instances for SPI calculation








            // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
            ExecutorService executor = Executors.newFixedThreadPool(n_threads);
            List<FutureTask> taskList = new ArrayList<FutureTask>();
            while (tdb.next()) {


                FutureTask futureTask_n = new FutureTask(new SPIEngineCallable("Thread-"+i,step,tdb.getDouble(3),tdb.getDouble(4),tdb.getInteger(1),tdb.getInteger(2)));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);

                i++;


            }

            // Wait until all results are available and combine them at the same time

            for (FutureTask futureTask : taskList) {
                resultFuture += (Long)futureTask.get();
            }

            System.out.println("Parallel threads have been finished");

            timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("ending time: "+timestamp);
            // Shutdown the ExecutorService

            executor.shutdown();

        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error ee "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error eee "+eee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{
            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }



        return Response.status(200).entity("SPI data updated!").build();

    }


    @POST
    @Path("/j_update_spi/{step}")
    public Response updateSPI(@PathParam("step") String step){

        TDBManager tdb=null;
        int width, height;
        int width_parc, height_parc;
        int imgt=7;
        int i=1;
        int n_threads=-1;
        int id_is=-1;

        Long resultFuture = new Long(0);

        try {

            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("SPI calculation, starting time: "+timestamp);

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            if(step.matches("3")){
                imgt=7;
            }else if(step.matches("6")){
                imgt=8;
            }else if(step.matches("12")){
                imgt=9;
            }

            //get the number of tiles and related threads
            sqlString = "select count(*), id_acquisizione " +
                    "   from   postgis.spi" + step + " " +
                    "   where  id_acquisizione = ( select min(id_acquisizione) from postgis.acquisizioni " +
                    "                              where id_imgtype = "+imgt+" ) " +
                    "   group by 2";

            //      System.out.println("SQL: "+ sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            if(tdb.next()){
                n_threads = tdb.getInteger(1);
                id_is = tdb.getInteger(2);
                System.out.println("n_threads: "+n_threads+ " ids: "+id_is);
            }else{

                System.out.println("Nothing was found");

                throw new Exception();
            }
            //get overall extent of dataset and ul coordinates



            sqlString=" select ST_Width(rast), ST_Height(rast), ST_UpperLeftX(rast), ST_UpperLeftY(rast) " +
                    "   from   postgis.spi" + step +
                    "   where  id_acquisizione = "+id_is;


            //  System.out.println("Get overall extent - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            //Launch parallel instances for SPI calculation








            // Create a new ExecutorService with 10 thread to execute and store the Futures. Each one represent one spi thread
            ExecutorService executor = Executors.newFixedThreadPool(n_threads);
            List<FutureTask> taskList = new ArrayList<FutureTask>();
            while (tdb.next()) {


                FutureTask futureTask_n = new FutureTask(new SPIEngineCallable("Thread-"+i,step,tdb.getDouble(3),tdb.getDouble(4),tdb.getInteger(1),tdb.getInteger(2)));

                taskList.add(futureTask_n);

                executor.execute(futureTask_n);

                i++;


            }

            // Wait until all results are available and combine them at the same time

            for (FutureTask futureTask : taskList) {
                resultFuture += (Long)futureTask.get();
            }

            System.out.println("Parallel threads have been finished");

            timestamp = new Timestamp(System.currentTimeMillis());
            System.out.println("ending time: "+timestamp);
            // Shutdown the ExecutorService

            executor.shutdown();

        }catch(SQLException sqle){
            System.out.println("Error  : "+sqle.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error ee "+ee.getMessage());
                try{


                    tdb.closeConnection();
                }catch (Exception eee){
                    System.out.println("Error eee "+eee.getMessage());
                }
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity("Error during import procedure!").build();

        } finally{
            try{


                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }



        return Response.status(200).entity("SPI data updated!").build();

    }



    /**
     * Method for update EVI and NDVI images from MODIS
     *
     * @param product       (MOD13Q1)
     * @param collection    (6)
     * @param north
     * @param south
     * @param east
     * @param west
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_evi/{product}/{collection}/{north}/{south}/{east}/{west}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateEviNdvi(@PathParam("product") String product,
                               @PathParam("collection") String collection,
                               @PathParam("north") String north,
                               @PathParam("south") String south,
                               @PathParam("east") String east,
                               @PathParam("west") String west,
                               @PathParam("year_in") String year_in,
                               @PathParam("month_in") String month_in,
                               @PathParam("day_in") String day_in,
                               @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        System.out.println("Start");
        try {
            if(year_in.matches("") || year_in == null){
            	GregorianCalendar gregorianCalendar=new GregorianCalendar();
            	
                System.out.println("Checking doy for year: "+year);
                tdb = new TDBManager("jdbc/ssdb");
                ///////

                
                //checking last doy and year
                sqlString = "SELECT extract(doy from max(dtime)), extract(year from max(dtime)) " + 
                		"FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype) " + 
                		"WHERE  imgtype = 'EVI' ";
                
                tdb.setPreparedStatementRef(sqlString);
                tdb.runPreparedQuery();

                if(tdb.next()){                	                	
                    if((tdb.getInteger(1)+16) > 366) {
                    	//move to next year
                    	year=""+(tdb.getInteger(2) + 1);
                    	doy = "1";
                    }else {
                    	year = ""+(tdb.getInteger(2));
                    	doy=""+(tdb.getInteger(1) + 16);
                    }
                        
                }else{
                    doy="1";
                   
                }
                gregorianCalendar.set(Calendar.YEAR,Integer.parseInt(year));
                gregorianCalendar.set(Calendar.DAY_OF_YEAR,Integer.parseInt(doy));

                month  = String.valueOf((gregorianCalendar.get(GregorianCalendar.MONTH) + 1));
                day    = String.valueOf(gregorianCalendar.get(GregorianCalendar.DAY_OF_MONTH));
                
            }else{
                month= month_in.split("/")[2];
                year= year_in.split("/")[2];
                day= day_in.split("/")[2];
                doy= doy_in.split("/")[2];



            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;


            System.out.println(""+year+"-"+month+"-"+day+"  "+doy);


            httpMng.setUrl(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+north+"&south="+south+"&east="+east+"&west="+west+"&product="+product+"&collection="+collection);
          

            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = new InputSource();
            is.setCharacterStream(new StringReader(httpMng.sendGet()));

            doc = db.parse(is);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            nList = doc.getElementsByTagName("return");

            if(nList.getLength()<1){
                System.out.println("No result");
            }else {
                fileList =  nList.item(0).getTextContent();

                for (int icount = 1; icount < nList.getLength(); icount++) {

                    fileList = fileList + "," + nList.item(icount).getTextContent();
                }
                System.out.println("IDs List: "+fileList);


                httpMng.setUrl(WS_MODIS_GETFILEURL+"fileIds="+fileList);
                is.setCharacterStream(new StringReader(httpMng.sendGet()));

                doc = db.parse(is);

                doc.getDocumentElement().normalize();

                System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

                nList = doc.getElementsByTagName("return");

                if(nList.getLength()<1){
                    System.out.println("No files found");
                }else {


                    for (int icount = 0; icount < nList.getLength(); icount++) {

                        System.out.println("Elem : "+nList.item(icount).getTextContent());

                        builder = new ProcessBuilder();
                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("wget","-O",TMP_DIR+"/prod"+icount+".hdf",nList.item(icount).getTextContent());

                        System.out.println("Starting shell procedure");

                        process = builder.start();

                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;

                    }

                    //Extracting EVI
                    System.out.println("extracting EVI...");

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/import_modis.sh",year,doy);

                    System.out.println("Starting shell procedure");

                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    System.out.println("Calculating VCI..."+doy+"-"+year);

                    sqlString = "select  postgis.calculate_vci_simple(?, ?)";
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.setParameter(DBManager.ParameterType.INT,doy,1);
                    tdb.setParameter(DBManager.ParameterType.INT,year,2);
                    tdb.runPreparedQuery();

                    if(tdb.next()){
                        System.out.println("Success.");
                    }else{
                        System.out.println("Attempt calculate VCI.");
                    }

                    System.out.println("Calculating E-VCI..."+doy+"-"+year);

                    sqlString = "select  postgis.calculate_evci_simple(?, ?)";
                    tdb.setPreparedStatementRef(sqlString);
                    tdb.setParameter(DBManager.ParameterType.INT,doy,1);
                    tdb.setParameter(DBManager.ParameterType.INT,year,2);
                    tdb.runPreparedQuery();

                    if(tdb.next()){
                        System.out.println("Success.");
                    }else{
                        System.out.println("Attempt calculate E-VCI.");
                    }
                    System.out.println("Done.");

                }
            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());

            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }

            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        return Response.status(200).entity("Image saved!").build();


    }


    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }


    /**
     * Method for update LST images from MODIS
     * @param product     (MOD11A2)
     * @param collection  (6)
     * @param north
     * @param south
     * @param east
     * @param west
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_lst/{product}/{collection}/{north}/{south}/{east}/{west}{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}{no_tci:(/no_tci/.+?)?}")
    public Response updateLST(@PathParam("product") String product,
                                  @PathParam("collection") String collection,
                                  @PathParam("north") String north,
                                  @PathParam("south") String south,
                                  @PathParam("east") String east,
                                  @PathParam("west") String west,
                                  @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("doy_in") String doy_in,
                                  @PathParam("no_tci") String no_tci){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        System.out.println("Starting j_update_lst procedure...");
        try {
            if(year_in.matches("") || year_in == null){
                GregorianCalendar gregorianCalendar=new GregorianCalendar();


               
                System.out.println("Checking doy for year: "+year);
                tdb = new TDBManager("jdbc/ssdb");
                ///////

                
                //checking last doy and year
                sqlString = "SELECT extract(doy from max(dtime)), extract(year from max(dtime)) " + 
                		"FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype) " + 
                		"WHERE  imgtype = 'LST' ";
                
                tdb.setPreparedStatementRef(sqlString);
                tdb.runPreparedQuery();

                if(tdb.next()){                	                	
                    if((tdb.getInteger(1)+8) > 366) {
                    	//move to next year
                    	year=""+(tdb.getInteger(2) + 1);
                    	doy = "1";
                    }else {
                    	year = ""+(tdb.getInteger(2));
                    	doy=""+(tdb.getInteger(1) + 8);
                    }
                        
                }else{
                    doy="1";
                   
                }
                gregorianCalendar.set(Calendar.YEAR,Integer.parseInt(year));
                gregorianCalendar.set(Calendar.DAY_OF_YEAR,Integer.parseInt(doy));

                month  = String.valueOf((gregorianCalendar.get(GregorianCalendar.MONTH) + 1));
                day    = String.valueOf(gregorianCalendar.get(GregorianCalendar.DAY_OF_MONTH));
            }else{
                GregorianCalendar gregorianCalendar=new GregorianCalendar();

                year= year_in.split("/")[2];

                doy= doy_in.split("/")[2];

               // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);

                gregorianCalendar.set(GregorianCalendar.YEAR,Integer.parseInt(year));
                gregorianCalendar.set(GregorianCalendar.DAY_OF_YEAR,Integer.parseInt(doy));

                month  = String.valueOf((gregorianCalendar.get(GregorianCalendar.MONTH) + 1));
                day    = String.valueOf(gregorianCalendar.get(GregorianCalendar.DAY_OF_MONTH));

                System.out.println("month: "+month+"  day: "+day);
                tdb = new TDBManager("jdbc/ssdb");
            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;


            System.out.println("Processing: "+year+"-"+month+"-"+day+"  "+doy);


            httpMng.setUrl(WS_MODIS_SEARCH4FILES+"start="+year+"-"+month+"-"+day+"&stop="+year+"-"+month+"-"+day+
                    "&coordsOrTiles=tiles&north="+north+"&south="+south+"&east="+east+"&west="+west+"&product="+product+"&collection="+collection);


            db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            is = new InputSource();
            is.setCharacterStream(new StringReader(httpMng.sendGet()));

            doc = db.parse(is);

            doc.getDocumentElement().normalize();

            System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

            nList = doc.getElementsByTagName("return");

            if(nList.getLength()<1){
                System.out.println("No result");
            }else {
                fileList =  nList.item(0).getTextContent();

                for (int icount = 1; icount < nList.getLength(); icount++) {

                    fileList = fileList + "," + nList.item(icount).getTextContent();
                }
                System.out.println("IDs List: "+fileList);


                httpMng.setUrl(WS_MODIS_GETFILEURL+"fileIds="+fileList);
                is.setCharacterStream(new StringReader(httpMng.sendGet()));

                doc = db.parse(is);

                doc.getDocumentElement().normalize();

                System.out.println("Root element :" + doc.getDocumentElement().getNodeName());

                nList = doc.getElementsByTagName("return");

                if(nList.getLength()<1){
                    System.out.println("No files found");
                }else {


                    for (int icount = 0; icount < nList.getLength(); icount++) {

                        System.out.println("Elem : "+nList.item(icount).getTextContent());

                        builder = new ProcessBuilder();
                        builder.redirectErrorStream(true);  //Redirect error on stdout

                        builder.command("wget","-O",TMP_DIR+"/prodlst"+icount+".hdf",nList.item(icount).getTextContent());

                        System.out.println("Starting shell procedure");
                        process = builder.start();

                        streamGobbler =
                                new StreamGobbler(process.getInputStream(), System.out::println);
                        Executors.newSingleThreadExecutor().submit(streamGobbler);

                        exitCode = process.waitFor();
                        assert exitCode == 0;

                    }

                    //Extracting EVI
                    System.out.println("extracting LST...");

                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("/usr/bin/import_lst.sh",year,doy);

                    System.out.println("Starting shell procedure");

                    process = builder.start();


                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;

                    if(no_tci.matches("") || no_tci == null) {
                    	System.out.println("Calculating TCI..."+doy+"-"+year);

                        sqlString = "select  postgis.calculate_tci(?, ?)";
                        tdb.setPreparedStatementRef(sqlString);
                        tdb.setParameter(DBManager.ParameterType.INT,doy,1);
                        tdb.setParameter(DBManager.ParameterType.INT,year,2);
                        tdb.runPreparedQuery();

                        if(tdb.next()){
                            System.out.println("Success.");
                        }else{
                            System.out.println("Attempt calculate TCI.");
                        }
                    }
                    System.out.println("Erase temp files");

                    builder = new ProcessBuilder();
                    builder.redirectErrorStream(true);  //Redirect error on stdout

                    builder.command("rm",TMP_DIR+"/prodlst*.hdf");

                    System.out.println("Starting shell procedure");
                    process = builder.start();

                    streamGobbler =
                            new StreamGobbler(process.getInputStream(), System.out::println);
                    Executors.newSingleThreadExecutor().submit(streamGobbler);

                    exitCode = process.waitFor();
                    assert exitCode == 0;
                    
                    System.out.println("Done.");

                }
            }


        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity("Error during import procedure!").build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }



    /**
     * Method for update VHI and E-VHI
     *
     * @param year_in
     * @param month_in
     * @param day_in
     * @param doy_in
     * @return
     */
    @GET
    @Path("/j_update_vhi{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{doy_in:(/doy_in/.+?)?}")
    public Response updateVHI(@PathParam("typ") String typ,
                              @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("doy_in") String doy_in){


        TDBManager tdb=null;
        String fileList = "",sqlString="";
        ProcessBuilder builder=null;
        HttpURLManager httpMng=new HttpURLManager();
        String year="";
        String month="";
        String day="";
        String doy="";
        Process process=null;
        DocumentBuilder db = null;
        InputSource is = null;
        Document doc=null;
        NodeList nList=null;
        StreamGobbler streamGobbler;
        int exitCode;
        List<String> arguments=null;

        System.out.println("Start");
        try {
            System.out.println("Updating vhi... "+typ);
            if(year_in.matches("") || year_in == null){
            	GregorianCalendar gregorianCalendar=new GregorianCalendar();


                
                System.out.println("Checking doy for year: "+year);
                tdb = new TDBManager("jdbc/ssdb");
                ///////

                
                //checking last doy and year
                sqlString = "SELECT extract(doy from max(dtime)), extract(year from max(dtime)) " + 
                		"FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype) " + 
                		"WHERE  imgtype = 'VHI' ";
                
                tdb.setPreparedStatementRef(sqlString);
                tdb.runPreparedQuery();

                if(tdb.next()){                	                	
                    if((tdb.getInteger(1)+16) > 366) {
                    	//move to next year
                    	year=""+(tdb.getInteger(2) + 1);
                    	doy = "1";
                    }else {
                    	year = ""+(tdb.getInteger(2));
                    	doy=""+(tdb.getInteger(1) + 16);
                    }
                        
                }else{
                    doy="1";
                   
                }
                gregorianCalendar.set(Calendar.YEAR,Integer.parseInt(year));
                gregorianCalendar.set(Calendar.DAY_OF_YEAR,Integer.parseInt(doy));

                month  = String.valueOf((gregorianCalendar.get(GregorianCalendar.MONTH) + 1));
                day    = String.valueOf(gregorianCalendar.get(GregorianCalendar.DAY_OF_MONTH));
            	
                //checking existence of VCI and TCI
                sqlString = "SELECT count(*) " + 
                		"FROM   postgis.acquisizioni INNER JOIN postgis.imgtypes USING (id_imgtype) " + 
                		"WHERE  imgtype in ('TCI', 'VCI') "+
                		"AND    extract(year from dtime)="+year+" "+
                		"AND    extract(doy from dtime) in ("+tdb.getInteger(1)+","+doy+")";
                tdb.setPreparedStatementRef(sqlString);
                tdb.runPreparedQuery();

                if(tdb.next()){                	                	
                    if(tdb.getInteger(1) < 3) {
                    	System.out.println("Missing data... procedure is goint to stop");
                    	tdb.closeConnection();
                    	return Response.status(500).entity("Missing data...!").build();
                    }
                        
                }
            }else{
                month= month_in.split("/")[2];
                year= year_in.split("/")[2];
                day= day_in.split("/")[2];
                doy= doy_in.split("/")[2];



            }

            if(Integer.parseInt(day)<10)
                day="0"+day;

            if(Integer.parseInt(month)<10)
                month="0"+month;

            System.out.println("Calculating VHI..."+doy+"-"+year);

            sqlString = "select  postgis.calculate_vhi(?, ?)";
            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,doy,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.runPreparedQuery();

            if(tdb.next()){
                System.out.println("Success.");
            }else{
                System.out.println("Attempt calculate VHI.");
            }

            System.out.println("Calculating E-VHI..."+doy+"-"+year);

            sqlString = "select  postgis.calculate_evhi(?, ?)";
            tdb.setPreparedStatementRef(sqlString);
            tdb.setParameter(DBManager.ParameterType.INT,doy,1);
            tdb.setParameter(DBManager.ParameterType.INT,year,2);
            tdb.runPreparedQuery();

            if(tdb.next()){
                System.out.println("Success.");
            }else{
                System.out.println("Attempt calculate E-VHI.");
            }
            System.out.println("Done.");



        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
               ee.printStackTrace();
            }


            return Response.status(500).entity("Error during import procedure!").build();

        }finally{
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }
    
    
    /**
     * Method for update CHIRPS rainfall images 
     * @param year_in
     * @param month_in
     * @return
     */
    @GET
    @Path("/j_update_chirps{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}")
    public Response updateCHIRPS( @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in){


        TDBManager tdb=null;
        String sqlString="";
        ProcessBuilder builder=null;
        int doy_count;

        Process process=null;
        StreamGobbler streamGobbler;
        int exitCode;
        String tblname;
        String year="", month="";
        
        System.out.println("Starting j_update_lst procedure...");
        try {
        
      
         
            tdb = new TDBManager("jdbc/ssdb");

            
           // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);
            if(year_in.matches("") || year_in == null){
            	
            	 System.out.print("Checking missing month... ");
            	 sqlString = "SELECT extract(month from max(dtime)), extract(year from max(dtime)) "+
            			 	"FROM postgis.acquisizioni "+
            			 	"WHERE id_imgtype = 1 ";
            	 
                 
                 tdb.setPreparedStatementRef(sqlString);

                 tdb.runPreparedQuery();
                 
                 
                 if(tdb.next()){
                	 year = ""+tdb.getInteger(2);
                     month =  String.format("%02d",(tdb.getInteger(1) + 1));
                     
                	 System.out.println(" "+month+ " "+year);
                 }else {
                	 try {
                         tdb.closeConnection();
                     } catch (SQLException ee) {
                         ee.printStackTrace();
                     }


                     return Response.status(500).entity("Attempt to get missing date").build();

                 }
                	 

                 
            }else {
            	System.out.print("Performing custom date: ");
            	month= String.format("%02d",Integer.parseInt(month_in.split("/")[2]));
                year= year_in.split("/")[2];
                
                System.out.println(" "+year+ " "+month);
            }
            
                      
            
      
            LocalDate convertedDate = LocalDate.parse((month+"/1/"+year), DateTimeFormatter.ofPattern("M/d/yyyy"));
            convertedDate = convertedDate.withDayOfMonth(
                                            convertedDate.getMonth().length(convertedDate.isLeapYear()));

            
            
            
            System.out.println("downloading CHIRPS "+convertedDate.getYear()+" "+convertedDate.getMonth()+
            		" with days: "+convertedDate.getDayOfMonth()+" ...");
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

            builder.command("wget","-O",TMP_DIR+"/chirps.nc", "-c",CHIRPS_GET_RAIN + year +"."+ month + CHIRPS_GET_RAIN2);

            System.out.println("Starting shell procedure");
            process = builder.start();

            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;
            
            for(int i=1; i<=convertedDate.getDayOfMonth(); i++){
            	System.out.println("extracting CHIRPS..."+i);

                builder.redirectErrorStream(true);  //Redirect error on stdout

                builder.command("/usr/bin/import_chirps.sh",""+i);



                process = builder.start();


                streamGobbler =
                        new StreamGobbler(process.getInputStream(), System.out::println);
                Executors.newSingleThreadExecutor().submit(streamGobbler);

                exitCode = process.waitFor();
                assert exitCode == 0;
                
              //check if there are no more images
            

                sqlString = "SELECT EXISTS (" +
                        "   SELECT 1" +
                        "   FROM   information_schema.tables " +
                        "   WHERE  table_schema = 'postgis' " +
                        "   AND    table_name = 'rain_flipped_warped' " +
                        "   );";
                System.out.print("checking images on db..."+sqlString );
                tdb.setPreparedStatementRef(sqlString);

                tdb.runPreparedQuery();


                if(tdb.next()){
                    if(tdb.getBoolean(1)){



                        System.out.println("Exists! check if exists dtime");

                        sqlString = "SELECT EXISTS (" +
                                "   select id_acquisizione " +
                                "   from   postgis.acquisizioni "+
                                "   where  dtime = to_timestamp('"+year+" "+month+" "+i+"', 'YYYY MM DD')" +
                                "   and    id_imgtype = 1);";
                        tdb.setPreparedStatementRef(sqlString);

                        tdb.runPreparedQuery();



                        if(tdb.next()) {
                            if (!tdb.getBoolean(1)) {
                                System.out.println("Doesn't exist! create new acquisizione");

                                sqlString = "insert into postgis.acquisizioni "+
                                        "(dtime, id_imgtype) "+
                                        "values "+
                                        "(to_timestamp('"+year+" "+month+" "+i+"', 'YYYY MM DD'),1)";

                                System.out.println(sqlString);

                                tdb.setPreparedStatementRef(sqlString);
                                tdb.performInsert();

                            }else{
                                System.out.println("Exists! use it!");

                            }
                        }


                        System.out.println("get new id_acquisizione");

                        sqlString = "select id_acquisizione "+
                                "from postgis.acquisizioni "+
                                "where id_imgtype = 1 "+
                                "and extract(year from dtime)="+year+" "+
                                "and extract(month from dtime)="+month+" "+
                                "and extract(day from dtime)="+i+" ";

                        System.out.println(sqlString);
                        tdb.setPreparedStatementRef(sqlString);
                        tdb.runPreparedQuery();

                        if(tdb.next()){
                            
                            System.out.println("insert new image");


                            sqlString="insert into postgis.precipitazioni "+
                                    "(rast, id_acquisizione) "+
                                    "select rast,"+tdb.getInteger(1)+" from "+
                                    "postgis.rain_flipped_warped";

                            System.out.println(sqlString);
                            tdb.setPreparedStatementRef(sqlString);

                            tdb.performInsert();

                        }else {
                        	System.out.println("SOMETHING WAS WRONG: there isn't any id_acquisizione for this image");                        	
                        }
                        sqlString="drop table postgis.rain_flipped_warped";


                        System.out.println("erase temp table");
                        tdb.setPreparedStatementRef(sqlString);

                        tdb.performInsert();
            	            	 
                    }
                }
            
            }
          
            
            
            System.out.println("deleting CHIRPS...");

            builder.redirectErrorStream(true);  //Redirect error on stdout

            builder.command("rm","-f", TMP_DIR+"/chirps.nc" );

            System.out.println("Starting shell procedure");

            process = builder.start();


            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;
            System.out.println("DONE.");
            
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity(e.getMessage()).build();

        }finally {
            try {
                tdb.closeConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }

    
    /**
     * Method for update CHIRPS rainfall images 
     * @param year_in
     * @param month_in
     * @return
     */
    @GET
    @Path("/j_update_nigerhyp{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{num_rec:(/num_rec/.+?)?}")
    public Response updateNigerHyp( @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("num_rec") String num_rec){


        TDBManager tdb=null;
        String sqlString="";
        ProcessBuilder builder=null;
        int doy_count;
        StringTokenizer strTk;
        HttpURLManager httpMng=new HttpURLManager();
        Process process=null;
        StreamGobbler streamGobbler;
        int exitCode;
        String tblname;
        String year="", month="";
        String day,num_r;

        List<String> csv_data = new ArrayList<>();
        
        
        Stream<String> stream2=null;
        Stream<String> stream=null;
        String retIssue;
        String retDataList;

        System.out.println("Starting j_update_nigerhyp procedure...");
        try {
        
      
         
            tdb = new TDBManager("jdbc/ssdb");

            
           // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);
           
        	System.out.print("Performing custom date: ");
        	month= String.format("%02d",Integer.parseInt(month_in.split("/")[2]));
            year= year_in.split("/")[2];
            num_r=num_rec.split("/")[2];
            day =  String.format("%02d",Integer.parseInt(day_in.split("/")[2]));
            
        
            final String thisfilter = year+"-"+month+"-"+day;
            final String thisfilter2 = year+month+day;
                      
            
      
       //     LocalDate convertedDate = LocalDate.parse((month+"/1/"+year), DateTimeFormatter.ofPattern("M/d/yyyy"));
       //     convertedDate = convertedDate.withDayOfMonth(
       //                                     convertedDate.getMonth().length(convertedDate.isLeapYear()));

           
            
        //    System.out.println("get nigerHyp link "+convertedDate.getYear()+"/"+convertedDate.getMonth()+"/" + convertedDate.getMonthValue());
            		
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

           
            
            builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe","-p","cat=[forecast,niger,hype,GFD13,ECOPER,!EOWL,!INSITU]","--pagination",""+num_r+"",
            		
            		"https://catalog.terradue.com/fanfar-00002/search","title,link:results"); 

            
            process = builder.start();

    //        streamGobbler =
    //                new StreamGobbler(process.getInputStream(), System.out::println);
    //        Executors.newSingleThreadExecutor().submit(streamGobbler);
            
            InputStream stdin = process.getInputStream(); 
            stream = new BufferedReader(new InputStreamReader(stdin)).lines();
        
            System.out.println("looking for: issued "+thisfilter+",https");
            
            retIssue = stream
 					.filter(line -> line.contains("issued "+thisfilter+",https")).findAny().orElse("nothing").toString();
 					
        
            
            
            exitCode = process.waitFor();
            assert exitCode == 0;
            
            
            stream.close(); 
           
            
            if(retIssue.matches("nothing")) {
            	System.out.println("not found, try later");           	
            }else {
            	System.out.println("Found: "+retIssue);
            	
            	retIssue = retIssue.substring((retIssue.indexOf(",")+1));
            	
             	
            	builder.redirectErrorStream(true);  //Redirect error on stdout    
                builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe",retIssue, "enclosure" );

                System.out.println("Extracting data list");

                process = builder.start();
                
                
                stdin = process.getInputStream(); 
                stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                
                
                
                System.out.println("looking for: 002_"+thisfilter2+"_1403_forecast_0004244.csv");
                retDataList= stream2
                			.filter(line -> line.contains("002_"+thisfilter2))
                			.filter(line -> line.contains("_forecast_0004244.csv"))
                			.findAny().orElse("nothing").toString();
                
                
                
                exitCode = process.waitFor();
                assert exitCode == 0;
                 
                
            	stream2.close();
                
                if(retDataList.matches("nothing")) {
                	
                	System.out.println("Found nothing, try later");
                }else {
                	System.out.println("Found: "+retDataList);
                	
                	
                	
                	builder.redirectErrorStream(true);  //Redirect error on stdo
                    builder.command("curl","-s",retDataList);

                    process = builder.start();

                    stdin = process.getInputStream(); 
                    stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                    
                    csv_data = stream2
                			.filter(line -> !line.contains("id,timestamp"))
                			.collect(Collectors.toList());
                    

                    
                    exitCode = process.waitFor();
                    assert exitCode == 0;
                          
                    stream2.close();
                    
                    
                    for(String element:csv_data) {
                    	System.out.println("Ecco: "+element);
                    }
                    	
                    	
                    	
                    	
                    if(csv_data.size() > 0) {
                    	 System.out.println("Preparing insert");
                    	
                    	 sqlString = "INSERT INTO postgis.niger_hyp_data (subbasin, dtime, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10) VALUES ('0004244','"+year+"-"+month+"-"+day+"',?,?,?,?,?,?,?,?,?,?)";
                         
                         tdb.setPreparedStatementRef(sqlString);
                         int icount = 0;
                         for(String element:csv_data){
                        	System.out.println("elaborazione : "+element);
                         	strTk = new StringTokenizer(element, ",");
                         	icount++;
                         	
                         	strTk.nextElement();
                         	strTk.nextElement();
                         	strTk.nextElement();
                         	strTk.nextElement();
                         	strTk.nextElement();
                         	     String aaaa =  ""+strTk.nextElement();
                         	     System.out.println(aaaa+" - "+icount);
                         	
                         	tdb.setParameter(ParameterType.DOUBLE, ""+aaaa, icount);
                         	             	
                         }
                         System.out.println("Performing insert");
                         tdb.performInsert();
                         
                
                    }else {
                    	System.out.println("no data fond");
                    	
                    	
                    }
                }
            }
            
            System.out.println("DONE");
            
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity(e.getMessage()).build();

        }finally {
            try {
            	
                tdb.closeConnection();
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }

    /**
     * Method for nigerhype
     * @param year_in
     * @param month_in
     * @return
     */
    
    
    @GET
    @Path("/j_update_nigerhyp2{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{num_rec:(/num_rec/.+?)?}{subbasin:(/subbasin/.+?)?}")
    public Response updateNigerHyp2( @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("num_rec") String num_rec,
                                  @PathParam("subbasin") String subbasin){


        TDBManager tdb=null;
        String sqlString="";
        ProcessBuilder builder=null;
        int doy_count;
        StringTokenizer strTk;
        HttpURLManager httpMng=new HttpURLManager();
        Process process=null;
        StreamGobbler streamGobbler;
        int exitCode;
        String tblname;
        String year="", month="";
        String day,num_r,subbasin_r;

        List<String> csv_data = new ArrayList<>();
        
        
        Stream<String> stream2=null;
        Stream<String> stream=null;
        String retIssue;
        String retDataList;

        System.out.println("Starting j_update_nigerhyp procedure...");
        try {
        
      
         
            tdb = new TDBManager("jdbc/ssdb");

            
           // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);
           
        	System.out.print("Performing custom date: ");
        	month= String.format("%02d",Integer.parseInt(month_in.split("/")[2]));
            year= year_in.split("/")[2];
            num_r=num_rec.split("/")[2];
            subbasin_r=subbasin.split("/")[2];
            day =  String.format("%02d",Integer.parseInt(day_in.split("/")[2]));
            
        
            final String thisfilter = year+"-"+month+"-"+day;
            final String thisfilter2 = year+month+day;
                      
            
      
       //     LocalDate convertedDate = LocalDate.parse((month+"/1/"+year), DateTimeFormatter.ofPattern("M/d/yyyy"));
       //     convertedDate = convertedDate.withDayOfMonth(
       //                                     convertedDate.getMonth().length(convertedDate.isLeapYear()));

           
            
        //    System.out.println("get nigerHyp link "+convertedDate.getYear()+"/"+convertedDate.getMonth()+"/" + convertedDate.getMonthValue());
            		
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

           
            
            builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe","-p","cat=[forecast,niger,hype,GFD13,ECOPER,!EOWL,!INSITU]","--pagination",""+num_r+"",
            		
            		"https://catalog.terradue.com/fanfar-00002/search","title,link:results"); 

            
            process = builder.start();

    //        streamGobbler =
    //                new StreamGobbler(process.getInputStream(), System.out::println);
    //        Executors.newSingleThreadExecutor().submit(streamGobbler);
            
            InputStream stdin = process.getInputStream(); 
            stream = new BufferedReader(new InputStreamReader(stdin)).lines();
        
            System.out.println("looking for: issued "+thisfilter+",https");
            
            retIssue = stream
 					.filter(line -> line.contains("issued "+thisfilter+",https")).findAny().orElse("nothing").toString();
 					
        
            
            
            exitCode = process.waitFor();
            assert exitCode == 0;
            
            
            stream.close(); 
           
            
            if(retIssue.matches("nothing")) {
            	System.out.println("not found, try later");           	
            }else {
            	System.out.println("Found: "+retIssue);
            	
            	retIssue = retIssue.substring((retIssue.indexOf(",")+1));
            	
             	
            	builder.redirectErrorStream(true);  //Redirect error on stdout    
                builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe",retIssue, "enclosure" );

                System.out.println("Extracting data list");

                process = builder.start();
                
                
                stdin = process.getInputStream(); 
                stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                
                
                
                System.out.println("looking for: 005_"+thisfilter2+"_1403_forecast_timeCOUT.txt");
                retDataList= stream2
                			.filter(line -> line.contains("_forecast_timeCOUT.txt"))
                			.findAny().orElse("nothing").toString();
                
                //.filter(line -> line.contains("_"+thisfilter2))
                
                exitCode = process.waitFor();
                assert exitCode == 0;
                 
                
            	stream2.close();
                
                if(retDataList.matches("nothing")) {
                	
                	System.out.println("Found nothing, try later");
                }else {
                	System.out.println("Found: "+retDataList);
                	
                	
                	
                	builder.redirectErrorStream(true);  //Redirect error on stdo
                    builder.command("curl","-s",retDataList);

                    process = builder.start();

                    stdin = process.getInputStream(); 
                    stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                    
                    csv_data = stream2
                			.filter(line -> !line.contains("!!"))
                			.collect(Collectors.toList());
                    

                    
                    exitCode = process.waitFor();
                    assert exitCode == 0;
                          
                    stream2.close();
                    
                    
                    for(String element:csv_data) {
                    	System.out.println("Ecco: "+element);
                    }
                    	
              //      Splitter splitter = Splitter.on('\t');
                    
                   
                    	
                    	
                    if(csv_data.size() > 0) {
                    	 System.out.println("Preparing insert");
                    	
                    	 sqlString = "INSERT INTO postgis.niger_hyp_data (subbasin, dtime, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10) VALUES ('000"+subbasin_r+"','"+year+"-"+month+"-"+day+"',?,?,?,?,?,?,?,?,?,?)";
                         
                         tdb.setPreparedStatementRef(sqlString);
                         int icount = 0, thisbasin=0, iicount=0;
                         for(String element:csv_data){
                        	 if(thisbasin==0) {
                        		 System.out.print("finding basin-column position...");
                        		 StringTokenizer st = new StringTokenizer(element, "\t");
                        		 System.out.print(""+st.countTokens());
                        		
                            	 while(st.hasMoreTokens() ){
                            		 icount++;
                            		 
                            		 if(st.nextToken().matches(subbasin_r)) {
                            			 thisbasin = icount;
                            			 icount=0;
                            			 System.out.println("..."+thisbasin);
                            			 break;
                            		 }                                
                                 }     
                        	 }else {
                        		 icount++;
                        		 iicount++;
                        		 StringTokenizer st = new StringTokenizer(element, "\t");
                        		 
                        		 while(st.hasMoreElements() ){
                        			 String str = st.nextToken();
                            		 if(iicount==thisbasin) {
                            			 System.out.println(str+" - "+icount);
                            			 tdb.setParameter(ParameterType.DOUBLE, str, icount);
                            			 iicount=0;
                            			 break;
                            		 }
                            	
                        			 iicount++;                       			 
                                 }     
                        	 }            	             	
                         }
                         System.out.println("Performing insert");
                         tdb.performInsert();
                         
                
                    }else {
                    	System.out.println("no data fond");
                    	
                    	
                    }
                }
            }
            
            System.out.println("DONE");
            
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity(e.getMessage()).build();

        }finally {
            try {
            	
                tdb.closeConnection();
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }
   
    
    /**
     * Method for nigerhype
     * @param year_in
     * @param month_in
     * @return
     */
    
    
    @GET
    @Path("/j_update_nigerhyp3{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{num_rec:(/num_rec/.+?)?}{subbasin:(/subbasin/.+?)?}")
    public Response updateNigerHyp3( @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("num_rec") String num_rec,
                                  @PathParam("subbasin") String subbasin){


        TDBManager tdb=null;
        String sqlString="";
        ProcessBuilder builder=null;
        int doy_count;
        StringTokenizer strTk;
        HttpURLManager httpMng=new HttpURLManager();
        Process process=null;
        StreamGobbler streamGobbler;
        int exitCode;
        String tblname;
        String year="", month="";
        String day,num_r,subbasin_r;

        List<String> csv_data = new ArrayList<>();
        
        
        Stream<String> stream2=null;
        Stream<String> stream=null;
        String retIssue;
        String retDataList;

        System.out.println("Starting j_update_nigerhyp procedure...");
        try {
        
      
         
            tdb = new TDBManager("jdbc/ssdb");

            
           // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);
           
        	System.out.print("Performing custom date: ");
        	month= String.format("%02d",Integer.parseInt(month_in.split("/")[2]));
            year= year_in.split("/")[2];
            num_r=num_rec.split("/")[2];
            subbasin_r=subbasin.split("/")[2];
            day =  String.format("%02d",Integer.parseInt(day_in.split("/")[2]));
            
        
            final String thisfilter = year+"-"+month+"-"+day;
            final String thisfilter2 = year+month+day;
                      
            
      
       //     LocalDate convertedDate = LocalDate.parse((month+"/1/"+year), DateTimeFormatter.ofPattern("M/d/yyyy"));
       //     convertedDate = convertedDate.withDayOfMonth(
       //                                     convertedDate.getMonth().length(convertedDate.isLeapYear()));

           
            
        //    System.out.println("get nigerHyp link "+convertedDate.getYear()+"/"+convertedDate.getMonth()+"/" + convertedDate.getMonthValue());
            		
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

           
            
            builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe","-p","cat=[forecast,niger,hype,GFD13,ECOPER,!EOWL,!INSITU]","--pagination",""+num_r+"",
            		
            		"https://catalog.terradue.com/fanfar-00002/search","title,link:results"); 

            
            process = builder.start();

    //        streamGobbler =
    //                new StreamGobbler(process.getInputStream(), System.out::println);
    //        Executors.newSingleThreadExecutor().submit(streamGobbler);
            
            InputStream stdin = process.getInputStream(); 
            stream = new BufferedReader(new InputStreamReader(stdin)).lines();
        
            System.out.println("looking for: issued "+thisfilter+",https");
            
            retIssue = stream
 					.filter(line -> line.contains("issued "+thisfilter+",https")).findAny().orElse("nothing").toString();
 					
        
            
            
            exitCode = process.waitFor();
            assert exitCode == 0;
            
            
            stream.close(); 
           
            
            if(retIssue.matches("nothing")) {
            	System.out.println("not found, try later");           	
            }else {
            	System.out.println("Found: "+retIssue);
            	
            	retIssue = retIssue.substring((retIssue.indexOf(",")+1));
            	
             	
            	builder.redirectErrorStream(true);  //Redirect error on stdout    
                builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe",retIssue, "enclosure" );

                System.out.println("Extracting data list");

                process = builder.start();
                
                
                stdin = process.getInputStream(); 
                stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                
                
                
                System.out.println("looking for: XXXX_1403_forecast_timeCOUT.txt");
                retDataList= stream2
                			.filter(line -> line.contains("_forecast_timeCOUT.txt"))
                			.filter(line -> line.contains("005"))
                			.findAny().orElse("nothing").toString();
                
                //.filter(line -> line.contains("_"+thisfilter2))
                
                exitCode = process.waitFor();
                assert exitCode == 0;
                 
                
            	stream2.close();
                
                if(retDataList.matches("nothing")) {
                	
                	System.out.println("Found nothing, try later");
                }else {
                	System.out.println("Found: "+retDataList);
                	
                	
                	
                	builder.redirectErrorStream(true);  //Redirect error on stdo
                    builder.command("curl","-s",retDataList);

                    process = builder.start();

                    stdin = process.getInputStream(); 
                    stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                    
                    csv_data = stream2
                			.filter(line -> !line.contains("!!"))
                			.collect(Collectors.toList());
                    

                    
                    exitCode = process.waitFor();
                    assert exitCode == 0;
                          
                    stream2.close();
                    
                    
                    for(String element:csv_data) {
                    	System.out.println("Ecco: "+element);
                    }
                    	
              //      Splitter splitter = Splitter.on('\t');
                    
                   
                    	
                    	
                    if(csv_data.size() > 0) {
                    	 System.out.println("Preparing insert");
                    	
                    	 sqlString = "INSERT INTO postgis.niger_hyp_data (subbasin, dtime,dtime2, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10) VALUES ('000"+subbasin_r+"','"+year+"-"+month+"-"+day+"',?,?,?,?,?,?,?,?,?,?,?)";
                         
                         tdb.setPreparedStatementRef(sqlString);
                         int icount = 0, thisbasin=0, iicount=0;
                         boolean foundissue=false;
                         for(String element:csv_data){
                        	 if(thisbasin==0) {
                        		 System.out.print("finding basin-column position...");
                        		 StringTokenizer st = new StringTokenizer(element, "\t");
                        		 System.out.print(""+st.countTokens());
                        		
                            	 while(st.hasMoreTokens() ){
                            		 icount++;
                            		 
                            		 if(st.nextToken().matches(subbasin_r)) {
                            			 thisbasin = icount;
                            			 icount=0;
                            			 System.out.println("..."+thisbasin);
                            			 break;
                            		 }                                
                                 }     
                        	 }else {
                        		 
                        		 
                        		 icount++;
                        		 iicount++;
                        		 StringTokenizer st = new StringTokenizer(element, "\t");
                        		 
                        		 while(st.hasMoreElements() ){
                        			 String str = st.nextToken();
                        		
                        			 if(!foundissue) {
                        				 foundissue=true;
                        				 tdb.setParameter(ParameterType.STRING, str, icount);
                        				 icount++;
                        			 }else {
                        				 if(iicount==thisbasin) {
                                			 System.out.println(str+" - "+icount);
                                			 tdb.setParameter(ParameterType.DOUBLE, str, icount);
                                			 iicount=0;
                                			 break;
                                		 }
                        			 }
                        			 
                            		
                            	
                        			 iicount++;                       			 
                                 }     
                        	 }            	             	
                         }
                         System.out.println("Performing insert");
                         tdb.performInsert();
                         
                
                    }else {
                    	System.out.println("no data fond");
                    	
                    	
                    }
                }
            }
            
            System.out.println("DONE");
            
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity(e.getMessage()).build();

        }finally {
            try {
            	
                tdb.closeConnection();
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }
   
    /**
     * Method for nigerhype
     * @param year_in
     * @param month_in
     * @return
     */
    
    
    @GET
    @Path("/j_update_worldhype{year_in:(/year_in/.+?)?}{month_in:(/month_in/.+?)?}{day_in:(/day_in/.+?)?}{num_rec:(/num_rec/.+?)?}{subbasin:(/subbasin/.+?)?}")
    public Response updateNigerHyp4( @PathParam("year_in") String year_in,
                                  @PathParam("month_in") String month_in,
                                  @PathParam("day_in") String day_in,
                                  @PathParam("num_rec") String num_rec,
                                  @PathParam("subbasin") String subbasin){


        TDBManager tdb=null;
        String sqlString="";
        ProcessBuilder builder=null;
        int doy_count;
        StringTokenizer strTk;
        HttpURLManager httpMng=new HttpURLManager();
        Process process=null;
        StreamGobbler streamGobbler;
        int exitCode;
        String tblname;
        String year="", month="";
        String day,num_r,subbasin_r;

        List<String> csv_data = new ArrayList<>();
        
        
        Stream<String> stream2=null;
        Stream<String> stream=null;
        String retIssue;
        String retDataList;

        System.out.println("Starting j_update_nigerhyp procedure...");
        try {
        
      
         
            tdb = new TDBManager("jdbc/ssdb");

            
           // System.out.println(""+year+"-"+month+"-"+day+"  "+doy);
           
        	System.out.print("Performing custom date: ");
        	month= String.format("%02d",Integer.parseInt(month_in.split("/")[2]));
            year= year_in.split("/")[2];
            num_r=num_rec.split("/")[2];
            subbasin_r=subbasin.split("/")[2];
            day =  String.format("%02d",Integer.parseInt(day_in.split("/")[2]));
            
        
            final String thisfilter = year+"-"+month+"-"+day;
            final String thisfilter2 = year+month+day;
                      
            
      
       //     LocalDate convertedDate = LocalDate.parse((month+"/1/"+year), DateTimeFormatter.ofPattern("M/d/yyyy"));
       //     convertedDate = convertedDate.withDayOfMonth(
       //                                     convertedDate.getMonth().length(convertedDate.isLeapYear()));

           
            
        //    System.out.println("get nigerHyp link "+convertedDate.getYear()+"/"+convertedDate.getMonth()+"/" + convertedDate.getMonthValue());
            		
            builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout

           
            
            builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe","-p","cat=[forecast,worldwidehype136,ecoper,hydrogfd20]","--pagination",""+num_r+"",
            		
            		"https://catalog.terradue.com/fanfar-00002/search","title,link:results"); 

            
            process = builder.start();

    //        streamGobbler =
    //                new StreamGobbler(process.getInputStream(), System.out::println);
    //        Executors.newSingleThreadExecutor().submit(streamGobbler);
            
            InputStream stdin = process.getInputStream(); 
            stream = new BufferedReader(new InputStreamReader(stdin)).lines();
        
            System.out.println("looking for: issued "+thisfilter+",https");
            
            retIssue = stream
            		.filter(line -> line.contains("[DI Out]"))
 					.filter(line -> line.contains("issued "+thisfilter+",https"))
 					.findAny().orElse("nothing").toString();
            
            
            
            exitCode = process.waitFor();
            assert exitCode == 0;
            
            
            stream.close(); 
           
            
            if(retIssue.matches("nothing")) {
            	System.out.println("not found, try later");           	
            }else {
            	System.out.println("Found: "+retIssue);
            	
            	retIssue = retIssue.substring((retIssue.indexOf(",")+1));
            	
             	
            	builder.redirectErrorStream(true);  //Redirect error on stdout    
                builder.command("/usr/bin/opensearch-client/OpenSearchClient.exe",retIssue, "enclosure" );

                System.out.println("Extracting data list");

                process = builder.start();
                
                
                stdin = process.getInputStream(); 
                stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                
                
                
                System.out.println("looking for: timeCOUT.txt");
                retDataList= stream2
                			.filter(line -> line.contains("_forecast_timeCOUT.txt"))
                			.findAny().orElse("nothing").toString();
                
                //.filter(line -> line.contains("_"+thisfilter2))
                
                exitCode = process.waitFor();
                assert exitCode == 0;
                 
                
            	stream2.close();
                
                if(retDataList.matches("nothing")) {
                	
                	System.out.println("Found nothing, try later");
                }else {
                	System.out.println("Found: "+retDataList);
                	
                	
                	
                	builder.redirectErrorStream(true);  //Redirect error on stdo
                    builder.command("curl","-s",retDataList);

                    process = builder.start();

                    stdin = process.getInputStream(); 
                    stream2 = new BufferedReader(new InputStreamReader(stdin)).lines();
                    
                    csv_data = stream2
                			.filter(line -> !line.contains("!!"))
                			.collect(Collectors.toList());
                    

                    
                    exitCode = process.waitFor();
                    assert exitCode == 0;
                          
                    stream2.close();
                    
                    
               //     for(String element:csv_data) {
               //     	System.out.println("Ecco: "+element);
               //     }
                    	
              //      Splitter splitter = Splitter.on('\t');
                    
                   
                    	
                    	
                    if(csv_data.size() > 0) {
                    	 System.out.println("Preparing insert");
                    	
                    	 sqlString = "INSERT INTO postgis.world_hype_data (subbasin, dtime,dtime2, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10) VALUES ('"+subbasin_r+"','"+year+"-"+month+"-"+day+"',?,?,?,?,?,?,?,?,?,?,?)";
                         
                         tdb.setPreparedStatementRef(sqlString);
                         int icount = 0, thisbasin=0, iicount=0;
                         boolean foundissue=false;
                         for(String element:csv_data){
                        	 if(thisbasin==0) {
                        		 System.out.print("finding basin-column position...");
                        		 StringTokenizer st = new StringTokenizer(element, "\t");
                        		 System.out.print(""+st.countTokens());
                        		
                            	 while(st.hasMoreTokens() ){
                            		 icount++;
                            		 
                            		 if(st.nextToken().matches(subbasin_r)) {
                            			 thisbasin = icount;
                            			 icount=0;
                            			 System.out.println("..."+thisbasin);
                            			 break;
                            		 }                                
                                 }     
                        	 }else {
                        		 
                        		 
                        		 icount++;
                        		 iicount++;
                        		 StringTokenizer st = new StringTokenizer(element, "\t");
                        		 
                        		 while(st.hasMoreElements() ){
                        			 String str = st.nextToken();
                        		
                        			 if(!foundissue) {
                        				 foundissue=true;
                        				 tdb.setParameter(ParameterType.STRING, str, icount);
                        				 icount++;
                        			 }else {
                        				 if(iicount==thisbasin) {
                                			 System.out.println(str+" - "+icount);
                                			 tdb.setParameter(ParameterType.DOUBLE, str, icount);
                                			 iicount=0;
                                			 break;
                                		 }
                        			 }
                        			 
                            		
                            	
                        			 iicount++;                       			 
                                 }     
                        	 }            	             	
                         }
                         System.out.println("Performing insert");
                         tdb.performInsert();
                         
                
                    }else {
                    	System.out.println("no data fond");
                    	
                    	
                    }
                }
            }
            
            System.out.println("DONE");
            
        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());
            try {
                tdb.closeConnection();
            } catch (SQLException ee) {
                ee.printStackTrace();
            }


            return Response.status(500).entity(e.getMessage()).build();

        }finally {
            try {
            	
                tdb.closeConnection();
                
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return Response.status(200).entity("Image saved!").build();


    }
}
