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


create table postgis.spatial_ref_sys
(
    srid integer not null
        constraint spatial_ref_sys_pkey
            primary key
        constraint spatial_ref_sys_srid_check
            check ((srid > 0) AND (srid <= 998999)),
    auth_name varchar(256),
    auth_srid integer,
    srtext varchar(2048),
    proj4text varchar(2048)
);

alter table postgis.spatial_ref_sys owner to postgres;

grant select on postgis.spatial_ref_sys to public;

grant select on postgis.spatial_ref_sys to simple_user;

create table postgis.migrations
(
    id serial
        constraint migrations_pkey
            primary key,
    migration varchar(255) not null,
    batch integer not null
);

alter table postgis.migrations owner to slapis;

grant select on postgis.migrations to simple_user;

create table postgis.users
(
    id serial
        constraint users_pkey
            primary key,
    name varchar(255) not null,
    email varchar(255) not null
        constraint users_email_unique
            unique,
    email_verified_at timestamp(0),
    password varchar(255) not null,
    remember_token varchar(100),
    created_at timestamp(0),
    updated_at timestamp(0),
    role varchar(100)
);

alter table postgis.users owner to slapis;

grant select on postgis.users to simple_user;

create table postgis.password_resets
(
    email varchar(255) not null,
    token varchar(255) not null,
    created_at timestamp(0)
);

alter table postgis.password_resets owner to slapis;

create index password_resets_email_index
    on postgis.password_resets (email);

grant select on postgis.password_resets to simple_user;

create table postgis.station_types
(
    _id_station_type serial
        constraint station_types_pkey
            primary key,
    stat_type varchar
);

alter table postgis.station_types owner to slapis;

grant select on postgis.station_types to simple_user;

create table postgis.rivers
(
    _id_river serial
        constraint rivers_pkey
            primary key,
    river varchar
);

alter table postgis.rivers owner to slapis;

grant select on postgis.rivers to simple_user;

create table postgis.countries
(
    _id_country serial
        constraint countries_pkey
            primary key,
    country varchar
);

alter table postgis.countries owner to slapis;

grant select on postgis.countries to simple_user;

create table postgis.stations
(
    _id_station serial
        constraint stations_pkey
            primary key,
    stat_name varchar,
    init_year integer,
    hydro_zero double precision,
    basin double precision,
    the_geom geometry(Point,4326),
    _id_country integer
        constraint stations__id_country_fkey
            references postgis.countries,
    _id_river integer
        constraint stations__id_river_fkey
            references postgis.rivers,
    _id_station_type integer
        constraint stations__id_station_type_fkey
            references postgis.station_types,
    code varchar(10),
    stat_id_str varchar,
    description varchar,
    bv varchar,
    riviere varchar
);

alter table postgis.stations owner to slapis;

grant select on postgis.stations to simple_user;

create table postgis.station_data
(
    _id_station_data serial
        constraint station_data_pkey
            primary key,
    _id_station integer
        constraint station_data__id_station_fkey
            references postgis.stations,
    dtime timestamp,
    depth double precision,
    battery double precision
);

alter table postgis.station_data owner to slapis;

grant select on postgis.station_data to simple_user;

create table postgis.scenarios
(
    gid serial
        constraint scenarios_pkey
            primary key,
    the_geom geometry(MultiPolygon,4326),
    basin_lvl varchar,
    name varchar(100),
    importance varchar(100),
    potential_damages varchar(100),
    impact varchar(100),
    color varchar(10),
    vigilance text,
    alert_plus1 text,
    alert_plus2 text,
    alert_plus3 text
);

alter table postgis.scenarios owner to slapis;

grant select on postgis.scenarios to simple_user;

create table postgis."0q10"
(
    gid serial
        constraint "0q10_pkey"
            primary key,
    fid integer,
    count integer,
    length numeric,
    area numeric,
    "range min" varchar(1),
    "range max" varchar(3),
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis."0q10" owner to slapis;

grant select on postgis."0q10" to simple_user;

create table postgis."1q5"
(
    gid serial
        constraint "1q5_pkey"
            primary key,
    fid integer,
    count integer,
    length numeric,
    area numeric,
    "range min" varchar(1),
    "range max" varchar(3),
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis."1q5" owner to slapis;

grant select on postgis."1q5" to simple_user;

create table postgis."2tr20"
(
    gid serial
        constraint "2tr20_pkey"
            primary key,
    fid integer,
    count integer,
    length numeric,
    area numeric,
    "range min" varchar(1),
    "range max" varchar(3),
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis."2tr20" owner to slapis;

grant select on postgis."2tr20" to simple_user;

create table postgis."3tr50"
(
    gid serial
        constraint "3tr50_pkey"
            primary key,
    fid integer,
    count integer,
    length numeric,
    area numeric,
    "range min" varchar(1),
    "range max" varchar(3),
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis."3tr50" owner to slapis;

grant select on postgis."3tr50" to simple_user;

create table postgis."4tr100"
(
    gid serial
        constraint "4tr100_pkey"
            primary key,
    fid integer,
    count integer,
    length numeric,
    area numeric,
    "range min" varchar(1),
    "range max" varchar(3),
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis."4tr100" owner to slapis;

grant select on postgis."4tr100" to simple_user;

create table postgis.thresholds
(
    _id_threshold serial
        constraint thresholds_pkey
            primary key,
    _id_station integer
        constraint thresholds__id_station_fkey
            references postgis.stations,
    depth_min double precision,
    depth_max double precision,
    coef_a double precision,
    coef_b double precision,
    h0 double precision default 0.0
);

alter table postgis.thresholds owner to slapis;

grant select on postgis.thresholds to simple_user;

create table postgis.idro_stations
(
    gid serial
        constraint idro_stations_pkey
            primary key,
    name varchar(254),
    country varchar(50),
    river varchar(50),
    basin_kmq double precision,
    instal_yea bigint,
    the_geom geometry(PointZM,32631)
);

alter table postgis.idro_stations owner to slapis;

grant select on postgis.idro_stations to simple_user;

create table postgis.sirba_idro
(
    gid serial
        constraint sirba_idro_pkey
            primary key,
    entity varchar(16),
    handle varchar(16),
    layer varchar(254),
    lyrfrzn integer,
    lyrlock integer,
    lyron integer,
    lyrvpfrzn integer,
    lyrhandle varchar(16),
    color integer,
    entcolor integer,
    lyrcolor integer,
    blkcolor integer,
    linetype varchar(254),
    entlinetyp varchar(254),
    lyrlntype varchar(254),
    blklinetyp varchar(254),
    elevation numeric,
    thickness numeric,
    linewt integer,
    entlinewt integer,
    lyrlinewt integer,
    blklinewt integer,
    refname varchar(254),
    ltscale numeric,
    extx numeric,
    exty numeric,
    extz numeric,
    docname varchar(254),
    docpath varchar(254),
    doctype varchar(32),
    docver varchar(16),
    the_geom geometry(MultiLineString,32631)
);

alter table postgis.sirba_idro owner to slapis;

grant select on postgis.sirba_idro to simple_user;

create table postgis.sirba_river
(
    gid serial
        constraint sirba_river_pkey
            primary key,
    entity varchar(16),
    handle varchar(16),
    layer varchar(254),
    lyrfrzn integer,
    lyrlock integer,
    lyron integer,
    lyrvpfrzn integer,
    lyrhandle varchar(16),
    color integer,
    entcolor integer,
    lyrcolor integer,
    blkcolor integer,
    linetype varchar(254),
    entlinetyp varchar(254),
    lyrlntype varchar(254),
    blklinetyp varchar(254),
    elevation numeric,
    thickness numeric,
    linewt integer,
    entlinewt integer,
    lyrlinewt integer,
    blklinewt integer,
    refname varchar(254),
    ltscale numeric,
    extx numeric,
    exty numeric,
    extz numeric,
    docname varchar(254),
    docpath varchar(254),
    doctype varchar(32),
    docver varchar(16),
    the_geom geometry(MultiLineString,32631)
);

alter table postgis.sirba_river owner to slapis;

grant select on postgis.sirba_river to simple_user;

create table postgis.basin_niger
(
    gid serial
        constraint basin_niger_pkey
            primary key,
    entity varchar(16),
    handle varchar(16),
    layer varchar(254),
    lyrfrzn integer,
    lyrlock integer,
    lyron integer,
    lyrvpfrzn integer,
    lyrhandle varchar(16),
    color integer,
    entcolor integer,
    lyrcolor integer,
    blkcolor integer,
    linetype varchar(254),
    entlinetyp varchar(254),
    lyrlntype varchar(254),
    blklinetyp varchar(254),
    elevation numeric,
    thickness numeric,
    linewt integer,
    entlinewt integer,
    lyrlinewt integer,
    blklinewt integer,
    refname varchar(254),
    ltscale numeric,
    extx numeric,
    exty numeric,
    extz numeric,
    docname varchar(254),
    docpath varchar(254),
    doctype varchar(32),
    docver varchar(16),
    name varchar(50),
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis.basin_niger owner to slapis;

grant select on postgis.basin_niger to simple_user;

create table postgis."0vert"
(
    gid serial
        constraint "0vert_pkey"
            primary key,
    layer varchar(254),
    seuil integer,
    q_mc_s double precision,
    index varchar(50),
    importance varchar(50),
    geom geometry(MultiPolygon,32631)
);

alter table postgis."0vert" owner to slapis;

grant select on postgis."0vert" to simple_user;

create table postgis."1jaune"
(
    gid serial
        constraint "1jaune_pkey"
            primary key,
    layer varchar(254),
    seuil integer,
    q_mc_s double precision,
    index varchar(50),
    importance varchar(50),
    geom geometry(MultiPolygon,32631)
);

alter table postgis."1jaune" owner to slapis;

grant select on postgis."1jaune" to simple_user;

create table postgis."2orange"
(
    gid serial
        constraint "2orange_pkey"
            primary key,
    layer varchar(254),
    seuil integer,
    q_mc_s double precision,
    index varchar(50),
    importance varchar(50),
    geom geometry(MultiPolygon,32631)
);

alter table postgis."2orange" owner to slapis;

grant select on postgis."2orange" to simple_user;

create table postgis."3rouge"
(
    gid serial
        constraint "3rouge_pkey"
            primary key,
    layer varchar(254),
    seuil integer,
    q_mc_s double precision,
    index varchar(50),
    importance varchar(50),
    geom geometry(MultiPolygon,32631)
);

alter table postgis."3rouge" owner to slapis;

grant select on postgis."3rouge" to simple_user;

create table postgis.check_level
(
    id integer default nextval('check_soglia_id_seq'::regclass) not null
        constraint check_soglia_pkey
            primary key,
    value integer,
    level char(10),
    dtime date,
    data_email date
);

alter table postgis.check_level owner to slapis;

grant select on postgis.check_level to simple_user;

create table postgis.scenarios_level
(
    id serial
        constraint scenarios_level_pkey
            primary key,
    lvl_min integer,
    lvl_max integer,
    basin_lvl varchar(50)
);

alter table postgis.scenarios_level owner to slapis;

grant select on postgis.scenarios_level to simple_user;

create table postgis.glofas
(
    dtime timestamp not null,
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
    _id_station integer not null
        constraint glofas__id_station_fkey
            references postgis.stations,
    constraint glofas_pkey
        primary key (dtime, _id_station)
);

alter table postgis.glofas owner to slapis;

grant select on postgis.glofas to simple_user;

create table postgis."0_vert_r2"
(
    gid serial
        constraint "0_vert_r2_pkey"
            primary key,
    scenario varchar(50),
    qmax_mc_s double precision,
    seuil integer,
    magnitude varchar(50),
    index_nstr integer,
    index_str integer,
    index_cdc integer,
    the_geom geometry(MultiPolygonM,32631)
);

alter table postgis."0_vert_r2" owner to slapis;

grant select on postgis."0_vert_r2" to simple_user;

create table postgis."1_jaune_r2"
(
    gid serial
        constraint "1_jaune_r2_pkey"
            primary key,
    scenario varchar(50),
    qmax_mc_s double precision,
    seuil integer,
    magnitude varchar(50),
    index_nstr integer,
    index_str integer,
    index_cdc integer,
    the_geom geometry(MultiPolygonM,32631)
);

alter table postgis."1_jaune_r2" owner to slapis;

grant select on postgis."1_jaune_r2" to simple_user;

create table postgis."2_orange_r2"
(
    gid serial
        constraint "2_orange_r2_pkey"
            primary key,
    scenario varchar(50),
    qmax_mc_s double precision,
    seuil integer,
    magnitude varchar(50),
    index_nstr integer,
    index_str integer,
    index_cdc integer,
    the_geom geometry(MultiPolygonM,32631)
);

alter table postgis."2_orange_r2" owner to slapis;

grant select on postgis."2_orange_r2" to simple_user;

create table postgis."3_rouge_r2"
(
    gid serial
        constraint "3_rouge_r2_pkey"
            primary key,
    scenario varchar(50),
    qmax_mc_s double precision,
    seuil integer,
    magnitude varchar(50),
    index_nstr integer,
    index_str integer,
    index_cdc integer,
    the_geom geometry(MultiPolygonM,32631)
);

alter table postgis."3_rouge_r2" owner to slapis;

grant select on postgis."3_rouge_r2" to simple_user;

create table postgis.basin_sirba
(
    gid serial
        constraint basin_sirba_pkey
            primary key,
    toponymie varchar(50),
    sup numeric,
    the_geom geometry(MultiPolygon,32631)
);

alter table postgis.basin_sirba owner to slapis;

grant select on postgis.basin_sirba to simple_user;

create table postgis.niger_hyp_subbasin
(
    subbasin varchar(8) not null
        constraint niger_hyp_subbasin_pkey
            primary key,
    the_geom geometry(Point,4326)
);

alter table postgis.niger_hyp_subbasin owner to slapis;

grant select on postgis.niger_hyp_subbasin to simple_user;

create table postgis.niger_hyp_data
(
    subbasin varchar
        constraint niger_hyp_data_subbasin_fkey
            references postgis.niger_hyp_subbasin,
    hn_id serial
        constraint niger_hyp_data_pkey
            primary key,
    dtime timestamp,
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
    dtime2 varchar
);

alter table postgis.niger_hyp_data owner to slapis;

create table postgis.world_hype_subbasin
(
    subbasin varchar(8) not null
        constraint world_hype_subbasin_pkey
            primary key,
    the_geom geometry(Point,4326),
    _id_station integer
        constraint world_hype_subbasin__id_station_fkey
            references postgis.stations
);

alter table postgis.world_hype_subbasin owner to slapis;

grant select on postgis.world_hype_subbasin to simple_user;

create table postgis.world_hype_data
(
    subbasin varchar
        constraint world_hype_data_subbasin_fkey
            references postgis.world_hype_subbasin,
    hn_id serial
        constraint world_hype_data_pkey
            primary key,
    dtime timestamp,
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
    dtime2 varchar
);

alter table postgis.world_hype_data owner to slapis;

create table postgis.niger_river
(
    gid serial
        constraint niger_river_pkey
            primary key,
    entity varchar(16),
    handle varchar(16),
    layer varchar(254),
    lyrfrzn integer,
    lyrlock integer,
    lyron integer,
    lyrvpfrzn integer,
    lyrhandle varchar(16),
    color integer,
    entcolor integer,
    lyrcolor integer,
    blkcolor integer,
    linetype varchar(254),
    entlinetyp varchar(254),
    lyrlntype varchar(254),
    blklinetyp varchar(254),
    elevation numeric,
    thickness numeric,
    linewt integer,
    entlinewt integer,
    lyrlinewt integer,
    blklinewt integer,
    refname varchar(254),
    ltscale numeric,
    extx numeric,
    exty numeric,
    extz numeric,
    docname varchar(254),
    docpath varchar(254),
    doctype varchar(32),
    docver varchar(16),
    nome varchar(50),
    the_geom geometry(MultiLineString,32631)
);

alter table postgis.niger_river owner to slapis;

create table postgis.out_of_rage
(
    _id_station integer not null
        constraint zeros__id_station_fkey
            references postgis.stations,
    off_value double precision,
    doy_from integer,
    doy_to integer
);

alter table postgis.out_of_rage owner to slapis;

create table postgis.station_graphic_thresholds
(
    id integer not null
        constraint station_graphic_thresholds_pkey
            primary key,
    _id_station integer not null
        constraint fk_id_station
            references postgis.stations,
    threshold_max integer not null,
    text_color varchar not null,
    graphic_type varchar not null,
    threshold_color varchar,
    threshold_min integer,
    alert varchar,
    cdc integer,
    tr integer,
    tr_ans integer
);

alter table postgis.station_graphic_thresholds owner to slapis;

