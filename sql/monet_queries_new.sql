-- Query 9a: Get data from vessels up to a certain number of ships
SELECT q1.*
FROM ais_data.dynamic_ships as q1
JOIN (
    SELECT DISTINCT mmsi
    FROM ais_data.dynamic_ships
    LIMIT 1250
) AS q2 ON q2.mmsi = q1.mmsi;
-- Query 9b: Get data from vessels up to a certain number of ships
SELECT q1.*
FROM ais_data.dynamic_ships as q1
JOIN (
    SELECT DISTINCT mmsi
    FROM ais_data.dynamic_ships
    LIMIT 2500
) AS q2 ON q2.mmsi = q1.mmsi;
-- Query 9c: Get data from vessels up to a certain number of ships
SELECT q1.*
FROM ais_data.dynamic_ships as q1
JOIN (
    SELECT DISTINCT mmsi
    FROM ais_data.dynamic_ships
    LIMIT 5055
) AS q2 ON q2.mmsi = q1.mmsi;
-- Query 10a: Get vessels within a certain interval (starting from the beginning)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE q1.t 
        BETWEEN 
            timestamp '2015-09-30 22:00:01.000000' 
            AND timestamp '2015-09-30 22:00:01.000000' + INTERVAL '45' DAY 
) AS q;
-- Query 10b: Get vessels within a certain interval (starting from the beginning)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE q1.t 
        BETWEEN 
            timestamp '2015-09-30 22:00:01.000000' 
            AND timestamp '2015-09-30 22:00:01.000000' + INTERVAL '91' DAY 
) AS q;
-- Query 10c: Get vessels within a certain interval (starting from the beginning)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE q1.t 
        BETWEEN 
            timestamp '2015-09-30 22:00:01.000000' 
            AND timestamp '2015-09-30 22:00:01.000000' + INTERVAL '183' DAY 
) AS q;
-- Query 11a: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM ais_data.dynamic_ships as q1
WHERE st_dwithin(
            q1.geom3035,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((3267481.3383331955 3059434.1646498768,
                              3468551.751719234 3079742.0776260556,
                              3436557.191444285 2881776.855171565,
                              3343053.3411638057 2948442.4396554087,
                              3225841.3695224654 2920784.2366986056,
                              3267481.3383331955 3059434.1646498768))'
                    )
                ,3035),
                0
        );
-- Query 11b: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM ais_data.dynamic_ships as q1
WHERE st_dwithin(
            q1.geom3035,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((3137111.871046437 3065669.809299177,
                              3228362.0872137905 2985259.028677077,
                              3266027.919269358 2881737.7109905197,
                              3087069.7277586637 2724076.501338935,
                              3065009.151042874 2738956.488339494,
                              3033098.8529121904 2907082.008469483,
                              3137111.871046437 3065669.809299177))'
                    )
                ,3035),
                0
        );
-- Query 11c: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM ais_data.dynamic_ships as q1
WHERE st_dwithin(
            q1.geom3035,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((3246320.3972688746 2871140.852367637,
                              3383688.855216654 2789037.021109264,
                              3365643.1156409904 2679208.566486555,
                              3225290.6801320994 2761734.1710481015,
                              3246320.3972688746 2871140.852367637))'
                    )
                ,3035),
            0
        );
-- Query 11d: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM ais_data.dynamic_ships as q1
WHERE st_dwithin(
            q1.geom3035,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((3270333.468477702 2913537.2105489373,
                              3271788.209424996 2897767.6264009275,
                              3234780.7698560935 2860903.721840171,
                              3209642.6806251267 2881771.300064762,
                              3236230.139035486 2918575.2581828474,
                              3270333.468477702 2913537.2105489373))'
                    )
                ,3035),
            0
        );
-- Query 11e: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM ais_data.dynamic_ships as q1
WHERE st_dwithin(
            q1.geom3035,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((2951501.933691309 3280081.578934718,
                              3228298.3396614464 3198747.5823778515,
                              3076901.3305201856 2481435.277154741,
                              2755965.824872542 2550216.8993245265,
                              2951501.933691309 3280081.578934718))'
                    )
                ,3035),
            0
        );