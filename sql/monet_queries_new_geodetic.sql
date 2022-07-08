-- Query 12a: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    JOIN poly1 as q2
    ON st_dwithingeographic(
            q1.geom,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-4.712363846038707 49.69307940236741,
                              -2 50.2,-2 48.4,
                              -3.404474593933708 48.84510724990374,
                              -4.9 48.4,-4.712363846038707 49.69307940236741))'
                    )
                ,4326),
            0
        )
) as q;
-- Query 12b: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    JOIN poly2 as q2
    ON st_dwithingeographic(
            q1.geom,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-6.5 49.5,
                              -5.039513262215193 48.970849308411616,
                              -4.266587582624421 48.12765765794896,
                              -6.177821990339785 46.41316796867488,
                              -6.5 46.5,-7.400449883510642 47.90280655115891,
                              -6.5 49.5))'
                    )
                ,4326),
            0
        )
) as q;
-- Query 12c: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    JOIN poly3 as q2
    ON st_dwithingeographic(
            q1.geom,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-4.5 48,
                              -2.5 47.5,
                              -2.5 46.5,
                              -4.5 47,
                              -4.5 48))'
                    )
                ,4326),
            0
        )
) as q;
-- Query 12d: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    JOIN poly4 as q2
    ON st_dwithingeographic(
            q1.geom,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-4.28995878425164 48.41508842473156,
                              -4.230688452998251 48.27876666284876,
                              -4.625823994687513 47.88955815428484,
                              -5.011081147834543 48.02785559387608,
                              -4.756218723444969 48.399283003063985,
                              -4.28995878425164 48.41508842473156))'
                    )
                ,4326),
            0
        )
) as q;
-- Query 12e: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    JOIN poly5 as q2
    ON st_dwithingeographic(
            q1.geom,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-9.765402631583392 50.950414439689965,
                              -5.65061469537787 50.84213054663192,
                              -5.677685668642381 44.263884043355986,
                              -9.792473604847903 44.18267112356246,
                              -9.765402631583392 50.950414439689965))'
                    )
                ,4326),
            0
        )
) as q;
-- Query 11a: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM ais_data.dynamic_ships as q1
WHERE st_dwithin2(
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
            q1.mbr,
            mbr(st_setsrid(
                st_geomfromtext(
                    'POLYGON((3270333.468477702 2913537.2105489373,
                              3271788.209424996 2897767.6264009275,
                              3234780.7698560935 2860903.721840171,
                              3209642.6806251267 2881771.300064762,
                              3236230.139035486 2918575.2581828474,
                              3270333.468477702 2913537.2105489373))'
                    )
                ,3035)),
            0
        );
-- Query 11b: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    ON st_dwithin2(
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
            q1.mbr,
            mbr(st_setsrid(
                st_geomfromtext(
                    'POLYGON((3270333.468477702 2913537.2105489373,
                              3271788.209424996 2897767.6264009275,
                              3234780.7698560935 2860903.721840171,
                              3209642.6806251267 2881771.300064762,
                              3236230.139035486 2918575.2581828474,
                              3270333.468477702 2913537.2105489373))'
                    )
                ,3035)),
            0
        )
) as q;
-- Query 11c: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    ON st_dwithin2(
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
            q1.mbr,
            mbr(st_setsrid(
                st_geomfromtext(
                    'POLYGON((3270333.468477702 2913537.2105489373,
                              3271788.209424996 2897767.6264009275,
                              3234780.7698560935 2860903.721840171,
                              3209642.6806251267 2881771.300064762,
                              3236230.139035486 2918575.2581828474,
                              3270333.468477702 2913537.2105489373))'
                    )
                ,3035)),
            0
        )
) as q;
-- Query 11d: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin2(
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
            q1.mbr,
            mbr(st_setsrid(
                st_geomfromtext(
                    'POLYGON((3270333.468477702 2913537.2105489373,
                              3271788.209424996 2897767.6264009275,
                              3234780.7698560935 2860903.721840171,
                              3209642.6806251267 2881771.300064762,
                              3236230.139035486 2918575.2581828474,
                              3270333.468477702 2913537.2105489373))'
                    )
                ,3035)),
            0
        )
) as q;
-- Query 11e: Get vessels within a bounded area (geometric dwithin using SRID:3035 projection)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin2(
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
            q1.mbr,
            mbr(st_setsrid(
                st_geomfromtext(
                    'POLYGON((3270333.468477702 2913537.2105489373,
                              3271788.209424996 2897767.6264009275,
                              3234780.7698560935 2860903.721840171,
                              3209642.6806251267 2881771.300064762,
                              3236230.139035486 2918575.2581828474,
                              3270333.468477702 2913537.2105489373))'
                    )
                ,3035)),
            0
        )
) as q;