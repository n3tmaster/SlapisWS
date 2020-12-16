#!/bin/sh

#create shape file for download

pgsql2shp -f /tmp/tomcat8-tomcat8-tmp/$1 -u slapis -P sl2019pwd  -g the_geom gisdb "SELECT * from postgis.$2 $3 $4"

zip -D -j /tmp/tomcat8-tomcat8-tmp/$1.zip /tmp/tomcat8-tomcat8-tmp/$1.shp /tmp/tomcat8-tomcat8-tmp/$1.shx /tmp/tomcat8-tomcat8-tmp/$1.dbf /tmp/tomcat8-tomcat8-tmp/$1.prj /tmp/tomcat8-tomcat8-tmp/$1.cpg
