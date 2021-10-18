SELECT st_distance(st_point(-4.782632,48.005634)::Geography,ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.4822764 48.115402,-4.348457 48.117886)')::Geography);
SELECT st_distance(st_point(-4.782632,48.005634)::Geography,ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886,-4.4822764 48.115402)')::Geography);
SELECT st_distance(st_point(-4.782632,48.005634)::Geography,ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.782632 48.015634)')::Geography);

-- Perpendicular
SELECT st_distance(st_point(-4.782632,48.005634)::Geography,ST_LineFromText('LINESTRING(-4.777982 47.99531,-4.782632 48.015634)')::Geography);

SELECT st_distance(st_point(-4.448457,48.217886)::Geography,ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))')::Geography);
SELECT st_distance(ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.348457 48.117886)')::Geography,ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886)')::Geography);
SELECT st_distance(ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.448457 48.217886)')::Geography,ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886)')::Geography);
SELECT st_distance(ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.448457 48.217886)')::Geography,ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))')::Geography);
SELECT st_distance(ST_PolygonFromText('POLYGON((-4.448457 48.217886,-4.782632 48.005634,-4.448457 48.217886))')::Geography,ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))')::Geography);


SELECT st_contains(ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))'),st_point(-4.448457,48.217886));