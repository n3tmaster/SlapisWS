package it.cnr.ibimet.slapisws;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import it.cnr.ibimet.dbutils.SWH4EConst;
import it.cnr.ibimet.dbutils.TDBManager;

import it.lr.libs.DBManager;

/**
 * Created by lerocchi on 30/01/2019.
 *
 * GetRaster
 *
 * retrieves vector data from postgis
 */
@Path("/vector")
public class GetVector extends Application implements SWH4EConst, ReclassConst{
	
	
	/**
     * Get hydro station list
     *
     *
     * @return 
     */
    @GET
    @Path("/j_get_stations/")
    @Produces("text/plain")
    public Response get_polygon() {

        TDBManager tdb=null;
        String imgOut="";

        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select st_astext(ST_Union(the_geom)) from postgis.stations" ;

            System.out.println("SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getString(1);
         //       System.out.println("Extracted polygon: "+imgOut);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                    return  Response.status(500).entity(ee.getMessage()).build();
                }


                return  Response.status(404).entity("No Data Found").build();
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


        return responseBuilder.build();
    }
	
	
	/**
     * Get hydro station list + metadata
     *
     *
     * @return 
     */
    @GET
    @Path("/j_get_stations_geojson/")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_poly_GeoJSON() {

        TDBManager tdb=null;
        String imgOut="";

        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select * from postgis.export_stations()" ;

            System.out.println("J_GET_STATIONS_GEOJSON - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getString(1);
            //    System.out.println("Extracted polygon: "+imgOut);
            }else{
                try{
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                    return  Response.status(500).entity(ee.getMessage()).build();
                }


                return  Response.status(404).entity("No Data Found").build();
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


        return responseBuilder.build();
    }
    
    /**
     * Get hydro scenarios + meta_data
     *
     *
     * @return 
     */
    @GET
    @Path("/j_get_scenario{lvl:(/lvl/.+?)?}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_scenarios_JSON(@PathParam("lvl") String lvl) {

        TDBManager tdb=null;
        String imgOut="";
        String lvl_sql="";
        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            if(lvl.matches("") || lvl == null) {
            	sqlString="select * from postgis.export_all_scenarios()" ;
            	
            	
            }else {
            	lvl_sql = lvl.split("/")[2];
            	sqlString="select * from postgis.export_scenario('"+lvl_sql+"')" ;
            }
            
            
           

            System.out.println("J_GET_SCENARIOS - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getString(1);
                //System.out.println("Extracted polygon: "+imgOut);
            }else{
                try{
                	
                	System.out.println("J_GET_SCENARIOS - Closing connection...");
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                    return  Response.status(500).entity(ee.getMessage()).build();
                }


                return  Response.status(404).entity("No Data Found").build();
            }




        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
            	System.out.println("J_GET_SCENARIOS - Closing connection...");
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {

            try{
            	System.out.println("J_GET_SCENARIOS - Closing connection...");
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }


        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);


        return responseBuilder.build();
    }
    
    
    /**
     * Get hydro scenarios + meta_data
     *
     *
     * @return 
     */
    @GET
    @Path("/j_get_layer/{layer_name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_layer_JSON(@PathParam("layer_name") String layer_name) {

        TDBManager tdb=null;
        String imgOut="";
        String lvl_sql="";
        try {

            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

           
            
            sqlString="select * from postgis.export_layer('"+layer_name+"')" ;
            
            
            
           

            System.out.println("J_GET_LAYER - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            if (tdb.next()) {
                imgOut = tdb.getString(1);
         //       System.out.println("Extracted polygon: "+imgOut);
            }else{
                try{
                	
                	System.out.println("J_GET_LAYER - Closing connection...");
                    tdb.closeConnection();
                }catch (Exception ee){
                    System.out.println("Error "+ee.getMessage());
                    return  Response.status(500).entity(ee.getMessage()).build();
                }


                return  Response.status(404).entity("No Data Found").build();
            }




        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
            	System.out.println("J_GET_LAYER - Closing connection...");
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

            return Response.status(500).entity(e.getMessage()).build();
        }finally {

            try{
            	System.out.println("J_GET_LAYER - Closing connection...");
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }


        Response.ResponseBuilder responseBuilder = Response.ok(imgOut);


        return responseBuilder.build();
    }
    
    @GET
    @Produces("application/zip")
    @Path("/j_shp_poly/{layer_name}/{tbl_name}{where:(/where/.+?)?}")
    public Response downloadShp(@PathParam("layer_name") String layer_name, @PathParam("tbl_name") String tbl_name, @PathParam("where") String where)
    
    {
    	
    	byte[] imgOut=null;
    	double ref_value=0.0,minval=0.0,maxval=0.0;
    	File fileout=null;
    	List<Double> listQuantile;
    	try {
	
    		String legend;
    		int id_ids;
    		ProcessBuilder builder=null;
    		ByteArrayOutputStream is;
    		Process process=null;
    		StreamGobbler streamGobbler;
    		
    		
    		int exitCode;
    	
			builder = new ProcessBuilder();
            builder.redirectErrorStream(true);  //Redirect error on stdout
            
			builder.command("rm","-f",System.getProperty("java.io.tmpdir")+"/*.zip");
        	
            System.out.println("J_POLY_NITRO - deleting old files");
            process = builder.start();

            streamGobbler =
                    new StreamGobbler(process.getInputStream(), System.out::println);
            Executors.newSingleThreadExecutor().submit(streamGobbler);

            exitCode = process.waitFor();
            assert exitCode == 0;
            
            if(where.matches("") || where == null) {
            	builder.command("/usr/bin/create_shpout.sh",layer_name,tbl_name);
            }else {
            	builder.command("/usr/bin/create_shpout.sh",layer_name,tbl_name, "where",where.split("/")[2]);
            }
	            
	
	            System.out.println("J_POLY_NITRO - Starting Polygonize porcedure...");
	            process = builder.start();
	
	            streamGobbler =
	                    new StreamGobbler(process.getInputStream(), System.out::println);
	            Executors.newSingleThreadExecutor().submit(streamGobbler);
	
	            exitCode = process.waitFor();
	            assert exitCode == 0;
            
	            System.out.println("J_POLY_NITRO - Reading shape from "+System.getProperty("java.io.tmpdir"));
	            fileout = new File(System.getProperty("java.io.tmpdir") + "/"+layer_name+".zip");
	            
	            builder.command("rm_shpout.sh",layer_name);
	        	
	            System.out.println("J_POLY_NITRO - deleting files");
	            process = builder.start();

	            streamGobbler =
	                    new StreamGobbler(process.getInputStream(), System.out::println);
	            Executors.newSingleThreadExecutor().submit(streamGobbler);

	            exitCode = process.waitFor();
	            assert exitCode == 0;
	            
	            
	    
			
	
	
	
	
    	}catch(Exception e){
    		System.out.println("Error  : "+e.getMessage());
	
	
    	
    		return Response.status(500).entity(e.getMessage()).build();
    	}finally {
	
    		
	
    	}
	
    	Response.ResponseBuilder responseBuilder = Response.ok((Object)fileout);
    	responseBuilder.header("Content-Disposition", "attachment; filename=\""+layer_name+".zip\"");
    	
    	return responseBuilder.build();

	
	
	

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
    
    
}
