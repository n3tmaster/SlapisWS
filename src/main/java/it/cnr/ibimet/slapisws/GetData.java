package it.cnr.ibimet.slapisws;

import java.text.SimpleDateFormat;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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
@Path("/data")
public class GetData extends Application implements SWH4EConst, ReclassConst{
	
	/**
     * Get hydro station list + metadata
     *
     *
     * @return 
     */
    @GET
    @Path("/j_get_stations_data/{id_station}/{n_data}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_station_data(@PathParam("id_station") String id_station,
    								@PathParam("n_data") String n_data) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,"
            		+ "depth, battery from postgis.station_data where _id_station = "+id_station+" LIMIT "+n_data ;

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("date", formatter.format(tdb.getData(1).getTime()));
                
                jobj.put("hour", formatter2.format(tdb.getData(1).getTime()));
                
                jobj.put("depth", tdb.getDouble(2));
                jobj.put("battery", tdb.getDouble(3));
                
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
	

    @GET
    @Path("/j_get_stations_alldata/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_station_data(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,"
            		+ "depth, battery from postgis.station_data where _id_station = "+id_station+" ORDER BY dtime desc ";

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("date", formatter.format(tdb.getData(1).getTime()));
                
                jobj.put("hour", formatter2.format(tdb.getData(1).getTime()));
                
                jobj.put("depth", tdb.getDouble(2));
                jobj.put("battery", tdb.getDouble(3));
                
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
    
    
    @GET
    @Path("/j_get_stations_alldata_csv/{id_station}")
    @Produces("text/csv")
    public Response get_station_data_csv(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";
        
        

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,"
            		+ "depth, battery from postgis.station_data where _id_station = "+id_station+" ORDER BY dtime desc ";

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            retData = "date, hour, depth, battery \n";

            while(tdb.next()){

                
            	retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime()) + "," +tdb.getDouble(2) + "," + tdb.getDouble(3) + "\n";
             
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
    
    @GET
    @Path("/j_get_glofas_rawdata/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_glofas_data(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,d25,d26,d27,d28,d29,d30 from postgis.glofas where _id_station = "+id_station
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("date", formatter.format(tdb.getData(1).getTime()));
                
                jobj.put("hour", formatter2.format(tdb.getData(1).getTime()));
                
                jobj.put("d1", tdb.getDouble(2));
         
                jobj.put("d2", tdb.getDouble(3));
                jobj.put("d3", tdb.getDouble(4));
                jobj.put("d4", tdb.getDouble(5));
                jobj.put("d5", tdb.getDouble(6));
                jobj.put("d6", tdb.getDouble(7));
                jobj.put("d7", tdb.getDouble(8));
                jobj.put("d8", tdb.getDouble(9));
                jobj.put("d9", tdb.getDouble(10));
                jobj.put("d10", tdb.getDouble(11));
                jobj.put("d11", tdb.getDouble(12));
                jobj.put("d12", tdb.getDouble(13));
                jobj.put("d13", tdb.getDouble(14));
                jobj.put("d14", tdb.getDouble(15));
                jobj.put("d15", tdb.getDouble(16));
                jobj.put("d16", tdb.getDouble(17));
                jobj.put("d17", tdb.getDouble(18));
                jobj.put("d18", tdb.getDouble(19));
                jobj.put("d19", tdb.getDouble(20));
                jobj.put("d20", tdb.getDouble(21));
                jobj.put("d21", tdb.getDouble(22));
                jobj.put("d22", tdb.getDouble(23));
                jobj.put("d23", tdb.getDouble(24));
                jobj.put("d24", tdb.getDouble(25));
                jobj.put("d25", tdb.getDouble(26));
                jobj.put("d26", tdb.getDouble(27));
                jobj.put("d27", tdb.getDouble(28));
                jobj.put("d28", tdb.getDouble(29));
                jobj.put("d29", tdb.getDouble(30));
                jobj.put("d30", tdb.getDouble(31));
                       
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
	
    
    @GET
    @Path("/j_get_niger_hyp_rawdata/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_niger_hyp_data(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.niger_hyp_data where subbasin = '"+id_station+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_RAWDATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("issue date", formatter.format(tdb.getData(1).getTime()));            
                jobj.put("issue hour", formatter2.format(tdb.getData(1).getTime()));
                jobj.put("date", tdb.getString(2));
                
                jobj.put("d1", tdb.getDouble(3));
         
                jobj.put("d2", tdb.getDouble(4));
                jobj.put("d3", tdb.getDouble(5));
                jobj.put("d4", tdb.getDouble(6));
                jobj.put("d5", tdb.getDouble(7));
                jobj.put("d6", tdb.getDouble(8));
                jobj.put("d7", tdb.getDouble(9));
                jobj.put("d8", tdb.getDouble(10));
                jobj.put("d9", tdb.getDouble(11));
                jobj.put("d10", tdb.getDouble(12));
                                      
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
	
    
    @GET
    @Path("/j_get_niger_hyp_rawdata/{id_station}/{year}/{month}/{day}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_niger_hyp_data(@PathParam("id_station") String id_station,
    		@PathParam("year") String year,
    		@PathParam("month") String month,
    		@PathParam("day") String day) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.niger_hyp_data where subbasin = '"+id_station+"'"
            		+ " and dtime = '"+year+"-"+month+"-"+day+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_RAWDATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("issue date", formatter.format(tdb.getData(1).getTime()));
                
                jobj.put("issue hour", formatter2.format(tdb.getData(1).getTime()));
                
                jobj.put("date", tdb.getString(2));
                jobj.put("d1", tdb.getDouble(3));
         
                jobj.put("d2", tdb.getDouble(4));
                jobj.put("d3", tdb.getDouble(5));
                jobj.put("d4", tdb.getDouble(6));
                jobj.put("d5", tdb.getDouble(7));
                jobj.put("d6", tdb.getDouble(8));
                jobj.put("d7", tdb.getDouble(9));
                jobj.put("d8", tdb.getDouble(10));
                jobj.put("d9", tdb.getDouble(11));
                jobj.put("d10", tdb.getDouble(12));
                           
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
    
    @GET
    @Path("/j_get_glofas_rawdata_csv/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_glofas_rawdata_csv(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,d25,d26,d27,d28,d29,d30  from postgis.glofas where _id_station = "+id_station
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            retData = "date, hour, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,d25,d26,d27,d28,d29,d30\n";

            while(tdb.next()){

                
            	retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime()) + "," +tdb.getDouble(2) + "," + tdb.getDouble(3) + ",";
             
            	retData = retData + ""+tdb.getDouble(4) + "," + tdb.getDouble(5) + "," +tdb.getDouble(6) + "," + tdb.getDouble(7) + ","+tdb.getDouble(8) + "," + tdb.getDouble(9) + ",";
            	retData = retData + ""+tdb.getDouble(10) + "," + tdb.getDouble(11) + "," +tdb.getDouble(12) + "," + tdb.getDouble(13) + ","+tdb.getDouble(14) + "," + tdb.getDouble(15) + ",";
            	retData = retData + ""+tdb.getDouble(16) + "," + tdb.getDouble(17) + "," +tdb.getDouble(18) + "," + tdb.getDouble(19) + ","+tdb.getDouble(20) + "," + tdb.getDouble(21) + ",";
            	retData = retData + ""+tdb.getDouble(22) + "," + tdb.getDouble(23) + "," +tdb.getDouble(24) + "," + tdb.getDouble(25) + ","+tdb.getDouble(26) + "," + tdb.getDouble(27) + ",";
            	retData = retData + ""+tdb.getDouble(28) + "," + tdb.getDouble(29) + "," +tdb.getDouble(30) + "," + tdb.getDouble(31) + "\n";
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

    @GET
    @Path("/j_get_optimized_glofas_csv/{year}/{id_station}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response get_optimized_glofas_csv(@PathParam("year") String year,
                                             @PathParam("id_station") String id_station) {

        TDBManager tdb=null,  tdb2=null;
        String retData="";
        boolean first;
        try {
            SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            tdb2 = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime from postgis.glofas where _id_station = "+id_station +" "
                    + " AND extract(year from dtime) = " + year
                    + " ORDER BY dtime desc ";

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            sqlString = "select * from optimize_glofas(?,?)";
            retData = "date, hour, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10\n";

            tdb2.setPreparedStatementRef(sqlString);
            while(tdb.next()){
                tdb2.setParameter(DBManager.ParameterType.INT, id_station,2);
                tdb2.setParameter(DBManager.ParameterType.DATE, tdb.getData(1),1);
                tdb2.runPreparedQuery();
                retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime())  + ",";
                first=true;
                while(tdb2.next()){

                    if(first) {
                        retData = retData + tdb2.getDouble(2);
                        first = false;
                    }else{
                        retData = retData + "," + tdb2.getDouble(2);
                    }
                }
                retData = retData + "\n";


            }










        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
            try{
                tdb2.closeConnection();
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

            try{
                tdb2.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);


        return responseBuilder.build();
    }

    @GET
    @Path("/j_get_optimized_wh_csv/{year}/{subbasin}")
    @Produces(MediaType.TEXT_PLAIN)
    public Response get_optimized_wh_csv(@PathParam("year") String year,
                                             @PathParam("subbasin") String subbasin) {

        TDBManager tdb=null,  tdb2=null;
        String retData="";
        boolean first;
        try {
            SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            tdb2 = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime from postgis.world_hype_data where subbasin = '"+subbasin +"' "
                    + " AND extract(year from dtime) = " + year
                    + " ORDER BY dtime desc ";

            System.out.println("J_GET_STATIONS_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);

            tdb.runPreparedQuery();

            sqlString = "select * from optimize_world_hype(?,?)";
            retData = "date, hour, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10\n";

            tdb2.setPreparedStatementRef(sqlString);
            while(tdb.next()){
                tdb2.setParameter(DBManager.ParameterType.STRING, subbasin,2);
                tdb2.setParameter(DBManager.ParameterType.DATE, tdb.getData(1),1);
                tdb2.runPreparedQuery();
                retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime())  + ",";
                first=true;
                while(tdb2.next()){

                    if(first) {
                        retData = retData + tdb2.getDouble(2);
                        first = false;
                    }else{
                        retData = retData + "," + tdb2.getDouble(2);
                    }
                }
                retData = retData + "\n";


            }










        }catch(Exception e){
            System.out.println("Error  : "+e.getMessage());


            try{
                tdb.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }
            try{
                tdb2.closeConnection();
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

            try{
                tdb2.closeConnection();
            }catch (Exception ee){
                System.out.println("Error "+ee.getMessage());
            }

        }


        Response.ResponseBuilder responseBuilder = Response.ok(retData);


        return responseBuilder.build();
    }


    @GET
    @Path("/j_get_niger_hyp_rawdata_csv/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_niger_hyp_rawdata_csv(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.niger_hyp_data where subbasin = '"+id_station+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_CSV_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            retData = "issue date,issue hour,date, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10\n";

            while(tdb.next()){

                
            	retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime()) + "," + tdb.getString(2) + ",";
            	retData = retData + "" + tdb.getDouble(3) + "," + tdb.getDouble(4) + ",";
             
            	retData = retData + ""+tdb.getDouble(5) + "," + tdb.getDouble(6) + "," +tdb.getDouble(7) + "," + tdb.getDouble(8) + ","+tdb.getDouble(9) + "," + tdb.getDouble(10) + ",";
            	retData = retData + ""+tdb.getDouble(11) + "," + tdb.getDouble(12) + "\n";
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
	
    
    @GET
    @Path("/j_get_niger_hyp_rawdata_csv/{id_station}/{year}/{month}/{day}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_niger_hyp_rawdata_csv(@PathParam("id_station") String id_station,
    		@PathParam("year") String year,
    		@PathParam("month") String month,
    		@PathParam("day") String day) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.niger_hyp_data where subbasin = '"+id_station+"' and dtime = '"+year+"-"+month+"-"+day+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_CSV_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            retData = "date, hour, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10\n";

            while(tdb.next()){

                
            	retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime()) + "," + tdb.getString(2) + ",";
            	
            			
            	retData = retData + "" +		tdb.getDouble(3) + "," + tdb.getDouble(4) + ",";
             
            	retData = retData + ""+tdb.getDouble(5) + "," + tdb.getDouble(6) + "," +tdb.getDouble(7) + "," + tdb.getDouble(8) + ","+tdb.getDouble(9) + "," + tdb.getDouble(10) + ",";
            	retData = retData + ""+tdb.getDouble(11) + "," + tdb.getDouble(12) + "\n";
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
    
    

    @GET
    @Path("/j_get_world_hype_rawdata/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_world_hype_data(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.world_hype_data where subbasin = '"+id_station+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_RAWDATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("issue date", formatter.format(tdb.getData(1).getTime()));            
                jobj.put("issue hour", formatter2.format(tdb.getData(1).getTime()));
                jobj.put("date", tdb.getString(2));
                
                jobj.put("d1", tdb.getDouble(3));
         
                jobj.put("d2", tdb.getDouble(4));
                jobj.put("d3", tdb.getDouble(5));
                jobj.put("d4", tdb.getDouble(6));
                jobj.put("d5", tdb.getDouble(7));
                jobj.put("d6", tdb.getDouble(8));
                jobj.put("d7", tdb.getDouble(9));
                jobj.put("d8", tdb.getDouble(10));
                jobj.put("d9", tdb.getDouble(11));
                jobj.put("d10", tdb.getDouble(12));
                                      
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
	
    @GET
    @Path("/j_get_world_hype_rawdata/{id_station}/{year}/{month}/{day}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_world_hype_data(@PathParam("id_station") String id_station,
    		@PathParam("year") String year,
    		@PathParam("month") String month,
    		@PathParam("day") String day) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.world_hype_data where subbasin = '"+id_station+"'"
            		+ " and dtime = '"+year+"-"+month+"-"+day+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_RAWDATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            
            JSONArray jArray= new JSONArray();

            while(tdb.next()){

                JSONObject jobj = new JSONObject();
                jobj.put("issue date", formatter.format(tdb.getData(1).getTime()));
                
                jobj.put("issue hour", formatter2.format(tdb.getData(1).getTime()));
                
                jobj.put("date", tdb.getString(2));
                jobj.put("d1", tdb.getDouble(3));
         
                jobj.put("d2", tdb.getDouble(4));
                jobj.put("d3", tdb.getDouble(5));
                jobj.put("d4", tdb.getDouble(6));
                jobj.put("d5", tdb.getDouble(7));
                jobj.put("d6", tdb.getDouble(8));
                jobj.put("d7", tdb.getDouble(9));
                jobj.put("d8", tdb.getDouble(10));
                jobj.put("d9", tdb.getDouble(11));
                jobj.put("d10", tdb.getDouble(12));
                           
                
                jArray.add(jobj);
            }

            retData = jArray.toJSONString();
            
            
            




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
    
    
    @GET
    @Path("/j_get_world_hype_rawdata_csv/{id_station}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_world_hype_rawdata_csv(@PathParam("id_station") String id_station) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.world_hype_data where subbasin = '"+id_station+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_CSV_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            retData = "issue date,issue hour,date, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10\n";

            while(tdb.next()){

                
            	retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime()) + "," + tdb.getString(2) + ",";
            	retData = retData + "" + tdb.getDouble(3) + "," + tdb.getDouble(4) + ",";
             
            	retData = retData + ""+tdb.getDouble(5) + "," + tdb.getDouble(6) + "," +tdb.getDouble(7) + "," + tdb.getDouble(8) + ","+tdb.getDouble(9) + "," + tdb.getDouble(10) + ",";
            	retData = retData + ""+tdb.getDouble(11) + "," + tdb.getDouble(12) + "\n";
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
	
    
    @GET
    @Path("/j_get_world_hype_rawdata_csv/{id_station}/{year}/{month}/{day}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response get_world_hype_rawdata_csv(@PathParam("id_station") String id_station,
    		@PathParam("year") String year,
    		@PathParam("month") String month,
    		@PathParam("day") String day) {

        TDBManager tdb=null;
        String retData="";

        try {
        	SimpleDateFormat formatter=new SimpleDateFormat("yyyy-MM-dd");
        	SimpleDateFormat formatter2=new SimpleDateFormat("hh:mm");
            tdb = new TDBManager("jdbc/ssdb");
            String sqlString=null;

            sqlString="select dtime,dtime2,"
            		+ "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10 from postgis.world_hype_data where subbasin = '"+id_station+"' and dtime = '"+year+"-"+month+"-"+day+"'"
            		+ " ORDER BY dtime desc ";

            System.out.println("J_GET_NIGER_HYP_CSV_DATA - SQL: "+sqlString);

            tdb.setPreparedStatementRef(sqlString);
            
            tdb.runPreparedQuery();

            retData = "date, hour, d1,  d2,d3,d4,d5,d6,d7,d8,d9,d10\n";

            while(tdb.next()){

                
            	retData = retData + "" +formatter.format(tdb.getData(1).getTime()) +"," + formatter2.format(tdb.getData(1).getTime()) + "," + tdb.getString(2) + ",";
            	
            			
            	retData = retData + "" +		tdb.getDouble(3) + "," + tdb.getDouble(4) + ",";
             
            	retData = retData + ""+tdb.getDouble(5) + "," + tdb.getDouble(6) + "," +tdb.getDouble(7) + "," + tdb.getDouble(8) + ","+tdb.getDouble(9) + "," + tdb.getDouble(10) + ",";
            	retData = retData + ""+tdb.getDouble(11) + "," + tdb.getDouble(12) + "\n";
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
