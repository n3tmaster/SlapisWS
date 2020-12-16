create or replace function postgis.export_stations()
RETURNS text AS
$$
DECLARE
  geojson_out text;
 BEGIN
select row_to_json(fc) INTO geojson_out
from (
    select
        'FeatureCollection' as "type",
        array_to_json(array_agg(f)) as "features"
    from (
        select
            'Feature' as "type",
            ST_AsGeoJSON(the_geom, 6) :: json as "geometry",
            (
                select json_strip_nulls(row_to_json(t))
                from (
                    select
                        _id_station,
                        stat_name,
                        basin
                ) t
            ) as "properties"
        from postgis.stations
        
    ) as f
) as fc;

return(geojson_out);
END;
$$
language 'plpgsql';



create or replace function postgis.export_all_scenarios()
RETURNS text AS
$$
DECLARE
  geojson_out text;
 BEGIN
select row_to_json(fc) INTO geojson_out
from (
    select
        'FeatureCollection' as "type",
        array_to_json(array_agg(f)) as "features"
    from (
        select
            'Feature' as "type",
            ST_AsGeoJSON(the_geom, 6) :: json as "geometry",
            (
                select json_strip_nulls(row_to_json(t))
                from (
                    select
                        ST_Area(the_geom),
                        basin_lvl
                ) t
            ) as "properties"
        from postgis.scenarios
        
    ) as f
) as fc;

return(geojson_out);
END;
$$
language 'plpgsql';


create or replace function postgis.export_scenario(lvl varchar)
RETURNS text AS
$$
DECLARE
  geojson_out text;
 BEGIN
select row_to_json(fc) INTO geojson_out
from (
    select
        'FeatureCollection' as "type",
        array_to_json(array_agg(f)) as "features"
    from (
        select
            'Feature' as "type",
            ST_AsGeoJSON(the_geom, 6) :: json as "geometry",
            (
                select json_strip_nulls(row_to_json(t))
                from (
                    select
                        ST_Area(the_geom),
                        basin_lvl
                ) t
            ) as "properties"
        from postgis.scenarios
        where basin_lvl = lvl
        
    ) as f
) as fc;

return(geojson_out);
END;
$$
language 'plpgsql';

create or replace function postgis.export_layer(lvl varchar)
RETURNS text AS
$$
DECLARE
  geojson_out text;
 BEGIN
	 
	 EXECUTE '
select row_to_json(fc) 
from (
    select
        ''FeatureCollection'' as "type",
        array_to_json(array_agg(f)) as "features"
    from (
        select
            ''Feature'' as "type",
            ST_AsGeoJSON(ST_Transform(the_geom,4326), 6) :: json as "geometry",
            (
                select json_strip_nulls(row_to_json(t))
                from (
                    select
                        ST_Area(ST_Transform(the_geom,4326))
                ) t
            ) as "properties"
        from postgis.'||lvl||'
        
        
    ) as f
) as fc'  INTO geojson_out;

return(geojson_out);
END;
$$
language 'plpgsql';


--
-- extract GLOFAS optimized data
create or replace function postgis.optimize_glofas(dtime_in timestamp, _id_station int)
RETURNS table (dtime_out timestamp, glofas_out double precision) AS
$$
DECLARE


  d1 double precision;
  dcheck INT;
  mcheck INT;
  icount INT;
  
  
  numr INT;
  res character varying;
  
BEGIN
	RAISE NOTICE 'Extract GLOFAS raw data';

	dtime_out := dtime_in - interval '1 day';
	FOR icount IN 1..10 LOOP
		EXECUTE 'SELECT d'||icount||' FROM postgis.glofas WHERE dtime = $1 and _id_station = $2'
	    INTO d1
        USING dtime_in,_id_station;
		dtime_out := dtime_out + interval '1 day';
		
		dcheck := extract(day from dtime_out);
		mcheck := extract(month from dtime_out);
		
        RAISE NOTICE 'Executing % - %',dtime_out, icount;
	
		IF mcheck = 6 THEN		
			
			glofas_out := ((((exp(1.0)^3.0453)*(1+d1)^132.5391)) -1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 0.0 THEN
				glofas_out := 0.0;
			END IF;
			
		ELSIF mcheck = 7 AND dcheck <= 10 THEN
			
			glofas_out := 119.6083 + (135470 * d1) + ( -14993000*d1^2.0);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 8.0 THEN
				glofas_out := 8.0;
			END IF;
			
		ELSIF mcheck = 7 AND dcheck <= 20 AND dcheck > 11 THEN
			glofas_out := 220.0813 + (234.5152 * d1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 6.0 THEN
				glofas_out := 6.0;
			END IF;
			
		ELSIF mcheck = 7 AND dcheck <= 31 AND dcheck > 21 THEN
			glofas_out := (749.5309 * d1) + (-298.9367 * d1^2.0) + (29.3423 * d1)^3.0 + (-0.7941 * d1^4.0);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 7.0 THEN
				glofas_out := 7.0;
			END IF;
		ELSIF mcheck = 8 AND dcheck <= 10 THEN
		
			glofas_out := 459.0704 + (-1139.1 * d1) + (2435.6 * d1)^2.0 + (-1510.9 * d1^3.0) + (357.6878 * d1)^4.0 + (-28.0248 * d1^5.0);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 120.0 THEN
				glofas_out := 120.0;
			END IF;
		ELSIF mcheck = 8 AND dcheck <= 20 AND dcheck > 11 THEN
		
			glofas_out := 350.4166 + (26.1874 * d1) + (-0.3584*d1^2.0);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 60.0 THEN
				glofas_out := 60.0;
			END IF;
		
		ELSIF mcheck = 8 AND dcheck <= 31 AND dcheck > 21 THEN
		
			glofas_out := 470.8202 + (5.4112 * d1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 150.0 THEN
				glofas_out := 150.0;
			END IF;
			
		ELSIF mcheck = 9 AND dcheck <= 10 THEN
		
			glofas_out :=((((exp(1.0)^5.7298)*(1+d1)^0.1463)) -1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 150.0 THEN
				glofas_out := 150.0;
			END IF;
		ELSIF mcheck = 9 AND dcheck <= 20 AND dcheck > 11 THEN
		
			glofas_out := 210.1604 + (4.8564 * d1) + (-0.0127*d1^2.0);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 150.0 THEN
				glofas_out := 150.0;
			END IF;
			
		ELSIF mcheck = 9 AND dcheck <= 31 AND dcheck > 21 THEN
		
			glofas_out := ((((exp(1.0)^3.5805)*(1+d1)^0.4432)) -1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 50.0 THEN
				glofas_out := 50.0;
			END IF;	
		ELSIF mcheck = 10 THEN
		
			glofas_out := (0.9664 * d1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 0.0 THEN
				glofas_out := 0.0;
			END IF;	
		ELSIF mcheck = 11 THEN
		
			glofas_out := (d1);
			
			IF glofas_out > 2400.0 THEN
				glofas_out := 2400.0;
			ELSIF glofas_out < 0.0 THEN
				glofas_out := 0.0;
			END IF;	
		END IF;
		
		
		RETURN NEXT;
	
	
	END LOOP;
	

END;
$$
language 'plpgsql';






-- FUNCTION: postgis.optimize_glofas(timestamp without time zone, integer)

-- DROP FUNCTION postgis.optimize_glofas(timestamp without time zone, integer);
-- New version with algorithms prepared in 2020
CREATE OR REPLACE FUNCTION postgis.optimize_glofas(
    dtime_in timestamp without time zone,
    _id_station integer)
    RETURNS TABLE(dtime_out timestamp without time zone, glofas_out double precision)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE

    d1 double precision;
    dcheck INT;
    mcheck INT;
    icount INT;


    numr INT;
    res character varying;

BEGIN
    RAISE NOTICE 'Extract GLOFAS raw data';

    dtime_out := dtime_in - interval '1 day';
    FOR icount IN 1..10 LOOP
            EXECUTE 'SELECT d'||icount||' FROM postgis.glofas WHERE dtime = $1 and _id_station = $2'
                INTO d1
                USING dtime_in,_id_station;
            dtime_out := dtime_out + interval '1 day';

            dcheck := extract(day from dtime_out);
            mcheck := extract(month from dtime_out);

            RAISE NOTICE 'Executing % - % for: %',dtime_out, icount, d1;

            IF mcheck = 6 THEN

                --glofas_out := ((((exp(1.0)^3.0453)*(1+d1)^132.5391)) -1);

                glofas_out := 57.0059570524724 + (21991.2810474194 * d1) + (-519044.596395844 * d1^2.0) + (4452644.13579541 * d1^3.0) + (-12372695.0879734 * d1^4.0);


                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 0.0 THEN
                    glofas_out := 0.0;
                END IF;

            ELSIF mcheck = 7 AND dcheck <= 10 THEN

                -- glofas_out := 119.6083 + (135470 * d1) + ( -14993000*d1^2.0);

                glofas_out := 133.893186567601 + (41711.0377936412 * d1) + ( -1094076.56095041 * d1^2.0) + (1747770.07564359 * d1^3.0);


                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 7.0 THEN
                    glofas_out := 7.0;
                END IF;

            ELSIF mcheck = 7 AND dcheck <= 20 AND dcheck > 10 THEN
                -- glofas_out := 220.0813 + (234.5152 * d1);

                glofas_out := 232.087608358047 + (-4662.08382478494 * d1) + (12470.5614105203 * d1^2.0);


                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 7.0 THEN
                    glofas_out := 7.0;
                END IF;

            ELSIF mcheck = 7 AND dcheck <= 31 AND dcheck > 20 THEN
                -- glofas_out := (749.5309 * d1) + (-298.9367 * d1^2.0) + (29.3423 * d1)^3.0 + (-0.7941 * d1^4.0);
                glofas_out := 273.247971831548 + (182.320964961982 * d1);
                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 7.0 THEN
                    glofas_out := 7.0;
                END IF;
            ELSIF mcheck = 8 AND dcheck <= 10 THEN

                --glofas_out := 459.0704 + (-1139.1 * d1) + (2435.6 * d1)^2.0 + (-1510.9 * d1^3.0) + (357.6878 * d1)^4.0 + (-28.0248 * d1^5.0);
                glofas_out := 440.093949582358 + (528.311981614102 * d1);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 120.0 THEN
                    glofas_out := 120.0;
                END IF;
            ELSIF mcheck = 8 AND dcheck <= 20 AND dcheck > 10 THEN

                --glofas_out := 350.4166 + (26.1874 * d1) + (-0.3584*d1^2.0);
                glofas_out := 373.188056030643 + (63.0608867770593 * d1) + ( -1.97892159194577 * d1^2.0) + (0.0155655674493522 * d1^3.0);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 60.0 THEN
                    glofas_out := 60.0;
                END IF;

            ELSIF mcheck = 8 AND dcheck <= 31 AND dcheck > 20 THEN
                --glofas_out := 470.8202 + (5.4112 * d1);
                glofas_out := 486.107525272311 + (-74.53060190522 * d1) + (5.40738155580814 * d1^2.0) + (-0.101482158839039 * d1^3.0) + (0.000521691259383321 * d1^4.0);


                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 150.0 THEN
                    glofas_out := 150.0;
                END IF;

            ELSIF mcheck = 9 AND dcheck <= 10 THEN

                --glofas_out :=((((exp(1.0)^5.7298)*(1+d1)^0.1463)) -1);
                glofas_out := 476.465293894257 + (1.30199472149463 * d1);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 150.0 THEN
                    glofas_out := 150.0;
                END IF;
            ELSIF mcheck = 9 AND dcheck <= 20 AND dcheck > 10 THEN

                --glofas_out := 210.1604 + (4.8564 * d1) + (-0.0127*d1^2.0);
                glofas_out := 282.576681392244 + (4.57846436510914 * d1) + (-0.0150832457296455 * d1^2.0);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 150.0 THEN
                    glofas_out := 150.0;
                END IF;

            ELSIF mcheck = 9 AND dcheck <= 30 AND dcheck > 20 THEN

                --glofas_out := ((((exp(1.0)^3.5805)*(1+d1)^0.4432)) -1);
                glofas_out := 153.764155488631 + (1.19180068046191 * d1);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 50.0 THEN
                    glofas_out := 50.0;
                END IF;
            ELSIF mcheck = 10 THEN

                --glofas_out := (0.9664 * d1);
                glofas_out := 2.32840382649726 + (0.618734592230589 * d1);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 0.0 THEN
                    glofas_out := 0.0;
                END IF;
            ELSIF mcheck = 11 OR mcheck = 12 OR (mcheck >= 1 AND mcheck <= 5) THEN

                glofas_out := (d1);

                IF glofas_out > 2400.0 THEN
                    glofas_out := 2400.0;
                ELSIF glofas_out < 0.0 THEN
                    glofas_out := 0.0;
                END IF;
            END IF;


            RETURN NEXT;


        END LOOP;


END;
$BODY$;

ALTER FUNCTION postgis.optimize_glofas(timestamp without time zone, integer)
    OWNER TO slapis;

GRANT EXECUTE ON FUNCTION postgis.optimize_glofas(timestamp without time zone, integer) TO slapis;

GRANT EXECUTE ON FUNCTION postgis.optimize_glofas(timestamp without time zone, integer) TO PUBLIC;

GRANT EXECUTE ON FUNCTION postgis.optimize_glofas(timestamp without time zone, integer) TO simple_user;

-- Oprimization model for
-- New version with algorithms prepared in 2020
CREATE OR REPLACE FUNCTION postgis.optimize_world_hype(
    dtime_in timestamp without time zone,
    subbasin_in character varying)
    RETURNS TABLE(dtime_out timestamp without time zone, wh_out double precision)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE

    d1 double precision;
    dcheck INT;
    mcheck INT;
    icount INT;


    numr INT;
    res character varying;

BEGIN
    RAISE NOTICE 'Extract WorldHype raw data';

    dtime_out := dtime_in - interval '1 day';
    FOR icount IN 1..10 LOOP
            EXECUTE 'SELECT d'||icount||' FROM postgis.world_hype_data WHERE dtime = $1 and subbasin = $2'
                INTO d1
                USING dtime_in,subbasin_in;
            dtime_out := dtime_out + interval '1 day';

            dcheck := extract(day from dtime_out);
            mcheck := extract(month from dtime_out);

            RAISE NOTICE 'Executing % - % for: %',dtime_out, icount, d1;

            IF mcheck = 6 THEN

                --glofas_out := ((((exp(1.0)^3.0453)*(1+d1)^132.5391)) -1);

                wh_out := 23.1536976230399 + (90.2109996555238 * d1) + (-9.67932610009686 * d1^2.0);


                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 0.0 THEN
                    wh_out := 0.0;
                END IF;

            ELSIF mcheck = 7 AND dcheck <= 10 THEN

                -- glofas_out := 119.6083 + (135470 * d1) + ( -14993000*d1^2.0);

                wh_out := 114.940976499537 + (8.68611989765234 * d1) ;


                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 7.0 THEN
                    wh_out := 7.0;
                END IF;

            ELSIF mcheck = 7 AND dcheck <= 20 AND dcheck > 10 THEN
                -- glofas_out := 220.0813 + (234.5152 * d1);

                wh_out := 195.233203506304 + (26.3481483770786 * d1) + (-0.519072595953438 * d1^2.0) + (0.00216238103433825 * d1^3.0);


                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 7.0 THEN
                    wh_out := 7.0;
                END IF;

            ELSIF mcheck = 7 AND dcheck <= 31 AND dcheck > 20 THEN
                -- glofas_out := (749.5309 * d1) + (-298.9367 * d1^2.0) + (29.3423 * d1)^3.0 + (-0.7941 * d1^4.0);
                wh_out := 468.947180748894 + (5.40714557235768 * d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 7.0 THEN
                    wh_out := 7.0;
                END IF;
            ELSIF mcheck = 8 AND dcheck <= 10 THEN

                --glofas_out := 459.0704 + (-1139.1 * d1) + (2435.6 * d1)^2.0 + (-1510.9 * d1^3.0) + (357.6878 * d1)^4.0 + (-28.0248 * d1^5.0);
                wh_out := 572.034619567275 + (-39.061605101627 * d1) + (1.238600318441 * d1^2.0);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 120.0 THEN
                    wh_out := 120.0;
                END IF;
            ELSIF mcheck = 8 AND dcheck <= 20 AND dcheck > 10 THEN

                --glofas_out := 350.4166 + (26.1874 * d1) + (-0.3584*d1^2.0);
                wh_out := 499.053589853266 + (-2.28795494440282 * d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 60.0 THEN
                    wh_out := 60.0;
                END IF;

            ELSIF mcheck = 8 AND dcheck <= 31 AND dcheck > 20 THEN
                --glofas_out := 470.8202 + (5.4112 * d1);
                wh_out := 703.652397511369 + (-10.9282230555502 * d1) + (0.0638554000014334 * d1^2.0);


                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 150.0 THEN
                    wh_out := 150.0;
                END IF;

            ELSIF mcheck = 9 AND dcheck <= 10 THEN

                --glofas_out :=((((exp(1.0)^5.7298)*(1+d1)^0.1463)) -1);
                wh_out := 520.679437008704 + (0.125485853573895 * d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 150.0 THEN
                    wh_out := 150.0;
                END IF;
            ELSIF mcheck = 9 AND dcheck <= 20 AND dcheck > 10 THEN

                --glofas_out := 210.1604 + (4.8564 * d1) + (-0.0127*d1^2.0);
                wh_out := 444.891547256744 + (-1.77453148759644 * d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 150.0 THEN
                    wh_out := 150.0;
                END IF;

            ELSIF mcheck = 9 AND dcheck <= 30 AND dcheck > 20 THEN

                --glofas_out := ((((exp(1.0)^3.5805)*(1+d1)^0.4432)) -1);
                wh_out := 64.0693517971259 + (9.11185995742591 * d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 50.0 THEN
                    wh_out := 50.0;
                END IF;
            ELSIF mcheck = 10 THEN

                --glofas_out := (0.9664 * d1);
                wh_out := -149.09742266144 + (34.228163120401 * d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 0.0 THEN
                    wh_out := 0.0;
                END IF;
            ELSIF mcheck = 11 OR mcheck = 12 OR (mcheck >= 1 AND mcheck <= 5) THEN

                wh_out := (d1);

                IF wh_out > 2400.0 THEN
                    wh_out := 2400.0;
                ELSIF wh_out < 0.0 THEN
                    wh_out := 0.0;
                END IF;
            END IF;


            RETURN NEXT;


        END LOOP;


END;
$BODY$;

-- function for observed data extraction
-- this procedure will extract data, between dtime_start and dtime_end, and perform gap filling
-- this version calculate Q parameter also
-- RETURN TABLE with data ordeed by id_station and dtime
-- Version 2.0 - 16-12-2020 - battery is not available
-- FUNCTION: postgis.extract_observed_data(timestamp without time zone, timestamp without time zone)
--

CREATE OR REPLACE FUNCTION postgis.observed_data(
    dtime_start timestamp without time zone,
    dtime_end timestamp without time zone)
    RETURNS TABLE(_id_station_out integer,
                  date_out timestamp without time zone,
                  depth_out double precision,
                  q_out double precision)
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE
    ROWS 1000
AS $BODY$
DECLARE
    record_i RECORD;
    record_out RECORD;
    coef_a_calc double precision;
    coef_b_calc double precision;
    h0_calc double precision;
BEGIN

    FOR record_i IN SELECT _id_station as ids  FROM postgis.stations WHERE _id_station_type = 1
        LOOP
            FOR record_out IN
                with data_ok as (
                    select date_trunc('hour',dtime) as dtime , depth
                    FROM postgis.station_data
                    WHERE dtime between dtime_start AND dtime_end
                      AND _id_station = record_i.ids
                      AND postgis.check_out_of_range(extract(doy from dtime)::integer,depth,record_i.ids)
                ), data_null as (
                    select generate_series(
                                   dtime_start::timestamp,
                                   dtime_end::timestamp,
                                   '1 hour'::interval
                               ) as dtime_null
                )
                SELECT dtime as date, depth as depth
                FROM data_ok
                UNION
                SELECT dtime_null as date, null
                FROM data_null
                WHERE dtime_null not in (select dtime from data_ok)
                  and (select count(*) from data_ok) > 0
                ORDER BY date DESC
                LOOP
                    _id_station_out := record_i.ids;

                    date_out := record_out.date;

                    IF record_out.depth IS NULL THEN
                        RAISE NOTICE 'NULL RECORD, % - %',_id_station_out, date_out;
                        depth_out := null;
                        q_out := null;
                    ELSEIF record_out.depth < 0.0 THEN
                        RAISE NOTICE 'depth is set to zero, % - %',_id_station_out, date_out;
                        depth_out := 0.0;
                        q_out := 0.0;
                    ELSE
                        depth_out := record_out.depth;

                        --checking coefficients
                        SELECT coef_a, coef_b, h0 into coef_a_calc , coef_b_calc, h0_calc
                        FROM postgis.thresholds
                        WHERE _id_station = record_i.ids
                          AND depth_min <= depth_out
                          AND depth_max > depth_out;
                        --caculate Q
                        q_out := coef_a_calc  * ((depth_out - h0_calc) / 100) ^ coef_b_calc ;
                        RAISE NOTICE '% - % - % coeff_a: % - coeff_b: % - Q: %',_id_station_out, date_out,depth_out,coef_a_calc,coef_b_calc,q_out;

                    END IF;

                    RETURN NEXT;
                END LOOP;
        END LOOP;


END;
$BODY$;



-- check_out_of_range
--  check if given value, and correlated date, is out of range
CREATE OR REPLACE FUNCTION postgis.check_out_of_range(
    doy_in integer,
    value_check double precision,
    ids integer)
    RETURNS boolean
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE
AS $BODY$
DECLARE
    val_2_check double precision;

BEGIN

    SELECT off_value INTO val_2_check
    FROM postgis.out_of_rage
    WHERE doy_in BETWEEN doy_from AND doy_to
    AND _id_station = ids;

    IF value_check > val_2_check THEN
        RETURN false;
    ELSE
        RETURN true;
    end if;

END;
$BODY$;


-- FUNCTION: postgis.optimize_glofas(timestamp without time zone, integer)

-- DROP FUNCTION postgis.optimize_glofas(timestamp without time zone, integer);
-- New version with algorithms prepared in 2020
-- this version apply the optimization algorithm to all dataset and return it with optimized values
CREATE OR REPLACE FUNCTION postgis.optimize_glofas_flat(
    _id_station_in integer)
    RETURNS TABLE(dtime_out timestamp without time zone,
                  d1_out double precision,d2_out double precision,
                  d3_out double precision,d4_out double precision,
                  d5_out double precision,d6_out double precision,
                  d7_out double precision,d8_out double precision,
                  d9_out double precision,d10_out double precision)
    LANGUAGE 'plpgsql'

    COST 100
    VOLATILE
    ROWS 1000
AS $BODY$
DECLARE

    d1 double precision;
    dcheck INT;
    mcheck INT;
    icount INT;
    lst_i RECORD;
    glofas_out double precision;
    numr INT;
    res character varying;

BEGIN
    RAISE NOTICE 'Extract GLOFAS raw data';


    FOR lst_i IN select dtime as this_time
                 from postgis.glofas
                 where _id_station = _id_station_in
                 order by dtime
        LOOP

        dtime_out := lst_i.this_time;
        FOR icount IN 1..10 LOOP
                EXECUTE 'SELECT d'||icount||' FROM postgis.glofas WHERE dtime = $1 and _id_station = $2'
                    INTO d1
                    USING lst_i.this_time,_id_station_in;


                dcheck := extract(day from dtime_out);
                mcheck := extract(month from dtime_out);

                RAISE NOTICE 'Executing % - % for: %',dtime_out, icount, d1;

                IF mcheck = 6 THEN

                    --glofas_out := ((((exp(1.0)^3.0453)*(1+d1)^132.5391)) -1);

                    glofas_out := 57.0059570524724 + (21991.2810474194 * d1) + (-519044.596395844 * d1^2.0) + (4452644.13579541 * d1^3.0) + (-12372695.0879734 * d1^4.0);


                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 0.0 THEN
                        glofas_out := 0.0;
                    END IF;

                ELSIF mcheck = 7 AND dcheck <= 10 THEN

                    -- glofas_out := 119.6083 + (135470 * d1) + ( -14993000*d1^2.0);

                    glofas_out := 133.893186567601 + (41711.0377936412 * d1) + ( -1094076.56095041 * d1^2.0) + (1747770.07564359 * d1^3.0);


                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 7.0 THEN
                        glofas_out := 7.0;
                    END IF;

                ELSIF mcheck = 7 AND dcheck <= 20 AND dcheck > 10 THEN
                    -- glofas_out := 220.0813 + (234.5152 * d1);

                    glofas_out := 232.087608358047 + (-4662.08382478494 * d1) + (12470.5614105203 * d1^2.0);


                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 7.0 THEN
                        glofas_out := 7.0;
                    END IF;

                ELSIF mcheck = 7 AND dcheck <= 31 AND dcheck > 20 THEN
                    -- glofas_out := (749.5309 * d1) + (-298.9367 * d1^2.0) + (29.3423 * d1)^3.0 + (-0.7941 * d1^4.0);
                    glofas_out := 273.247971831548 + (182.320964961982 * d1);
                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 7.0 THEN
                        glofas_out := 7.0;
                    END IF;
                ELSIF mcheck = 8 AND dcheck <= 10 THEN

                    --glofas_out := 459.0704 + (-1139.1 * d1) + (2435.6 * d1)^2.0 + (-1510.9 * d1^3.0) + (357.6878 * d1)^4.0 + (-28.0248 * d1^5.0);
                    glofas_out := 440.093949582358 + (528.311981614102 * d1);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 120.0 THEN
                        glofas_out := 120.0;
                    END IF;
                ELSIF mcheck = 8 AND dcheck <= 20 AND dcheck > 10 THEN

                    --glofas_out := 350.4166 + (26.1874 * d1) + (-0.3584*d1^2.0);
                    glofas_out := 373.188056030643 + (63.0608867770593 * d1) + ( -1.97892159194577 * d1^2.0) + (0.0155655674493522 * d1^3.0);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 60.0 THEN
                        glofas_out := 60.0;
                    END IF;

                ELSIF mcheck = 8 AND dcheck <= 31 AND dcheck > 20 THEN
                    --glofas_out := 470.8202 + (5.4112 * d1);
                    glofas_out := 486.107525272311 + (-74.53060190522 * d1) + (5.40738155580814 * d1^2.0) + (-0.101482158839039 * d1^3.0) + (0.000521691259383321 * d1^4.0);


                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 150.0 THEN
                        glofas_out := 150.0;
                    END IF;

                ELSIF mcheck = 9 AND dcheck <= 10 THEN

                    --glofas_out :=((((exp(1.0)^5.7298)*(1+d1)^0.1463)) -1);
                    glofas_out := 476.465293894257 + (1.30199472149463 * d1);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 150.0 THEN
                        glofas_out := 150.0;
                    END IF;
                ELSIF mcheck = 9 AND dcheck <= 20 AND dcheck > 10 THEN

                    --glofas_out := 210.1604 + (4.8564 * d1) + (-0.0127*d1^2.0);
                    glofas_out := 282.576681392244 + (4.57846436510914 * d1) + (-0.0150832457296455 * d1^2.0);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 150.0 THEN
                        glofas_out := 150.0;
                    END IF;

                ELSIF mcheck = 9 AND dcheck <= 30 AND dcheck > 20 THEN

                    --glofas_out := ((((exp(1.0)^3.5805)*(1+d1)^0.4432)) -1);
                    glofas_out := 153.764155488631 + (1.19180068046191 * d1);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 50.0 THEN
                        glofas_out := 50.0;
                    END IF;
                ELSIF mcheck = 10 THEN

                    --glofas_out := (0.9664 * d1);
                    glofas_out := 2.32840382649726 + (0.618734592230589 * d1);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 0.0 THEN
                        glofas_out := 0.0;
                    END IF;
                ELSIF mcheck = 11 OR mcheck = 12 OR (mcheck >= 1 AND mcheck <= 5) THEN

                    glofas_out := (d1);

                    IF glofas_out > 2400.0 THEN
                        glofas_out := 2400.0;
                    ELSIF glofas_out < 0.0 THEN
                        glofas_out := 0.0;
                    END IF;
                END IF;


                CASE icount
                    WHEN 1 THEN
                        d1_out := glofas_out;
                    WHEN 2 THEN
                        d2_out := glofas_out;
                    WHEN 3 THEN
                        d3_out := glofas_out;
                    WHEN 4 THEN
                        d4_out := glofas_out;
                    WHEN 5 THEN
                        d5_out := glofas_out;
                    WHEN 6 THEN
                        d6_out := glofas_out;
                    WHEN 7 THEN
                        d7_out := glofas_out;
                    WHEN 8 THEN
                        d8_out := glofas_out;
                    WHEN 9 THEN
                        d9_out := glofas_out;
                    WHEN 10 THEN
                        d10_out := glofas_out;
                END CASE;


            END LOOP;

            RETURN NEXT;
        END LOOP;



END;
$BODY$;
CREATE OR REPLACE FUNCTION postgis.analysis1(
    dtime_start timestamp without time zone,
    dtime_end timestamp without time zone,
    id_station_in integer)
    RETURNS TABLE(
                     date_1 timestamp without time zone,
                     date_2 timestamp without time zone,
                     n_hours integer,
                     q_min_o double precision,
                     q_max_o double precision,
                     q_avg double precision,
                     phase varchar)
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE
    ROWS 1000
AS $BODY$
DECLARE

    record_out RECORD;
    coef_a_calc double precision;
    coef_b_calc double precision;

    actual_dtime timestamp;
    prev_dtime timestamp;
    actual_phase varchar;
    prev_phase varchar;
    actual_q double precision;
    actual_num integer;
    sum_q double precision;
    first boolean;
    actual_phase_num integer;
    q_max double precision;
    q_min double precision;

BEGIN
    first := true;
    actual_num:=0;
    actual_q:=0.0;


    FOR record_out IN
        with data_ok as (
            select date_trunc('hour',dtime) as dtime , depth  ,battery
            FROM postgis.station_data
            WHERE dtime between dtime_start AND dtime_end
              AND _id_station = id_station_in
        ), data_null as (
            select generate_series(
                           dtime_start::timestamp,
                           dtime_end::timestamp,
                           '1 hour'::interval
                       ) as dtime_null
        )
        SELECT dtime as date, depth as depth, battery as battery
        FROM data_ok
        UNION
        SELECT dtime_null as date, null, null
        FROM data_null
        WHERE dtime_null not in (select dtime from data_ok)
          and (select count(*) from data_ok) > 0
        ORDER BY date DESC
        LOOP
            RAISE NOTICE 'process: %',record_out.date;





            IF record_out.depth > 0.0 THEN


                --checking coefficients
                SELECT coef_a, coef_b into coef_a_calc , coef_b_calc
                FROM postgis.thresholds
                WHERE _id_station = id_station_in
                  AND depth_min <= record_out.depth
                  AND depth_max >= record_out.depth;
                --caculate Q
                actual_q := coef_a_calc  * (record_out.depth / 100) ^ coef_b_calc ;
                --  RAISE NOTICE '% - % - % coeff_a: % - coeff_b: % - Q: %',_id_station_out, date_out,depth_out,coef_a_calc,coef_b_calc,q_out;

            END IF;

            IF first = true THEN
                RAISE NOTICE 'start';
                first := false;
                actual_dtime := record_out.date;
                prev_dtime :=actual_dtime;
                actual_num:=1;
                IF actual_q >= 0.0 AND actual_q < 600.0 THEN
                    actual_phase := 'GREEN';
                    actual_phase_num := 1;
                ELSEIF actual_q >= 600.0 AND actual_q < 800.0 THEN
                    actual_phase := 'YELLOW';
                    actual_phase_num := 2;
                ELSEIF actual_q >= 800.0 AND actual_q < 1500.0 THEN
                    actual_phase := 'ORANGE';
                    actual_phase_num := 3;
                ELSEIF actual_q >= 1500.0 THEN
                    actual_phase = 'RED';
                    actual_phase_num := 4;
                end if;
                prev_phase := actual_phase;
                sum_q := actual_q;
                q_max := actual_q;
                q_min := actual_q;

            else
                IF actual_q >= 0.0 AND actual_q < 600.0 THEN
                    actual_phase := 'GREEN';
                ELSEIF actual_q >= 600.0 AND actual_q < 800.0 THEN
                    actual_phase := 'YELLOW';
                ELSEIF actual_q >= 800.0 AND actual_q < 1500.0 THEN
                    actual_phase := 'ORANGE';
                ELSEIF actual_q >= 1500.0 THEN
                    actual_phase := 'RED';
                end if;

                IF prev_phase = actual_phase THEN

                    actual_dtime := record_out.date;
                    actual_num:=actual_num+1;
                    sum_q := sum_q + actual_q;
                    IF actual_q > q_max THEN
                        q_max := actual_q;
                    end if;

                    IF actual_q < q_min THEN
                        q_min := actual_q;
                    end if;
                ELSE
                    date_1 := prev_dtime;
                    date_2 := actual_dtime;
                    n_hours := actual_num;
                    q_max_o := q_max;
                    q_min_o := q_min;
                    q_avg := sum_q / actual_num;
                    phase := prev_phase;

                    prev_dtime := actual_dtime;
                    actual_num := 1;
                    sum_q := actual_q;
                    prev_phase := actual_phase;
                    q_max := actual_q;
                    q_min := actual_q;

                    RETURN NEXT;
                end if;

            end if;


        END LOOP;




END;
$BODY$;


--  extract_observed_data
--  this version will extract data of given station and year
--  INPUT
--   year_in
--   id_station_in
--  OUTPUT
--   date_out
--   depth_out
--   battery_out
--   q_out
CREATE OR REPLACE FUNCTION postgis.extract_observed_data(
    year_in integer,
    id_station_in integer)
    RETURNS TABLE(date_out timestamp without time zone,
                  depth_out double precision,
                  battery_out double precision,
                  q_out double precision)
    LANGUAGE 'plpgsql'

    COST 100
    IMMUTABLE
    ROWS 1000
AS $BODY$
DECLARE

    record_out RECORD;
    coef_a_calc double precision;
    coef_b_calc double precision;

    dtime1 varchar;
    dtime2 varchar;
BEGIN

    dtime1 := year_in||'-01-01';
    dtime2 := current_date;
    FOR record_out IN
        with data_ok as (
            select date_trunc('hour',dtime) as dtime , depth  ,battery
            FROM postgis.station_data
            WHERE extract(year from dtime) = year_in
              AND _id_station = id_station_in
        ), data_null as (
            select generate_series(
                           dtime1::timestamp,
                           dtime2::timestamp,
                           '1 hour'::interval
                       ) as dtime_null
        )
        SELECT dtime as date, depth as depth, battery as battery
        FROM data_ok
        UNION
        SELECT dtime_null as date, null, null
        FROM data_null
        WHERE dtime_null not in (select dtime from data_ok)
          and (select count(*) from data_ok) > 0
        ORDER BY date DESC
        LOOP

            date_out := record_out.date;
            battery_out := record_out.battery;

            IF record_out.depth IS NULL THEN
                RAISE NOTICE 'NULL RECORD, % - %',id_station_in, date_out;
                depth_out := null;
                q_out := null;
            ELSEIF record_out.depth < 0.0 THEN
                RAISE NOTICE 'depth is set to zero, % - %',id_station_in, date_out;
                depth_out := 0.0;
                q_out := 0.0;
            ELSE
                depth_out := record_out.depth;

                --checking coefficients
                SELECT coef_a, coef_b into coef_a_calc , coef_b_calc
                FROM postgis.thresholds
                WHERE _id_station = id_station_in
                  AND depth_min <= depth_out
                  AND depth_max >= depth_out;
                --caculate Q
                q_out := coef_a_calc  * (depth_out / 100) ^ coef_b_calc ;
                RAISE NOTICE '% - % - % coeff_a: % - coeff_b: % - Q: %',id_station_in, date_out,depth_out,coef_a_calc,coef_b_calc,q_out;

            END IF;

            RETURN NEXT;
        END LOOP;



END;
$BODY$;