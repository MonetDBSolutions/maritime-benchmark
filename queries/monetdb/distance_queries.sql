-- Distance Tests
-- Point to Point
SELECT st_distancegeographic(st_point(-4.782632,48.005634),st_point(-4.46572,48.382507));
SELECT st_distancegeographic(st_point(-4.782632,48.005634),st_point(-4.348457,48.117886));

-- Point to Line
SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886)'));
SELECT st_distancegeographic(st_point(-4.448457,48.217886),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886)'));
-- These two should return the same value
SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402)'));
SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.4822764 48.115402,-4.348457 48.117886)'));

SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.782632 48.015634)'));

-- Perpendicular
SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_LineFromText('LINESTRING(-4.777982 47.99531,-4.782632 48.015634)'));

-- Point to Polygon
SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))'));
SELECT st_distancegeographic(st_point(-4.448457,48.217886),ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))'));
SELECT st_distancegeographic(st_point(-4.413,48.181),ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))'));

-- Perpendicular
SELECT st_distancegeographic(st_point(-4.782632,48.005634),ST_PolygonFromText('POLYGON((-4.777982 47.99531,-4.782632 48.015634, -4.777982 47.99531))'));


-- Line to Line
SELECT st_distancegeographic(ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.348457 48.117886)'),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886)'));
SELECT st_distancegeographic(ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.448457 48.217886)'),ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402)'));
SELECT st_distancegeographic(ST_LineFromText('LINESTRING(-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402)'),ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.448457 48.217886)'));

-- Line to Polygon
SELECT st_distancegeographic(ST_LineFromText('LINESTRING(-4.782632 48.005634,-4.448457 48.217886)'),ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))'));

-- Polygon to Polygon
SELECT st_distancegeographic(ST_PolygonFromText('POLYGON((-4.448457 48.217886,-4.782632 48.005634,-4.448457 48.217886))'),ST_PolygonFromText('POLYGON((-4.46572 48.382507,-4.348457 48.117886, -4.4822764 48.115402,-4.46572 48.382507))'));

