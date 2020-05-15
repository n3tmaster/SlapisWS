create table postgis.station_types(_id_station_type serial primary key,
								  stat_type varchar);
								  
create table postgis.rivers(_id_river serial primary key,
								  river varchar);	
								  
create table postgis.countries(_id_country serial primary key,
								  country varchar);	

create table postgis.stations (_id_station serial primary key,
							  stat_name varchar,
							  init_year integer,
							  hydro_zero double precision,
							  basin double precision,
							  the_geom geometry(point, 4326),
							  _id_country integer references postgis.countries(_id_country),
							  _id_river integer references postgis.rivers(_id_river),
							  _id_station_type integer references postgis.station_types(_id_station_type));
							  
create table postgis.station_data (_id_station_data serial primary key,
							  _id_station integer references postgis.stations(_id_station),
							  dtime timestamp,
							  depth double precision,
							  battery double precision);
							  
							  
create table postgis.scenarios (gid serial primary key,
							   the_geom geometry(MultiPolygon, 4326),
							   basin_lvl varchar);

create table postgis.glofas (dtime timestamp,
							   d1 double precision,
							   d2 double precision,
							   d3 double precision,
							   d4 double precision,
							   d5 double precision,
							   d6 double precision,
							   d7 double precision,
							   d8 double precision,
							   d9 double precision,
							   d10 double precision,
							   d11 double precision,
							   d12 double precision,
							   d13 double precision,
							   d14 double precision,
							   d15 double precision,
							   d16 double precision,
							   d17 double precision,
							   d18 double precision,
							   d19 double precision,
							   d20 double precision,
							   d21 double precision,
							   d22 double precision,
							   d23 double precision,
							   d24 double precision,
							   d25 double precision,
							   d26 double precision,
							   d27 double precision,
							   d28 double precision,
							   d29 double precision,
							   d30 double precision,
							   d31 double precision,
								_id_station integer references postgis.stations (_id_station),
								primary key(dtime, _id_station));