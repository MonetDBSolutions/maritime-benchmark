-- Query 12a: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin(
            q1.geom::geography,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-4.712363846038707 49.69307940236741,
                              -2 50.2,-2 48.4,
                              -3.404474593933708 48.84510724990374,
                              -4.9 48.4,-4.712363846038707 49.69307940236741))'
                    )
                ,4326)::geography,
            0
        )
) as q;
-- Query 12b: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin(
            q1.geom::geography,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-6.5 49.5,
                              -5.039513262215193 48.970849308411616,
                              -4.266587582624421 48.12765765794896,
                              -6.177821990339785 46.41316796867488,
                              -6.5 46.5,-7.400449883510642 47.90280655115891,
                              -6.5 49.5))'
                    )
                ,4326)::geography,
            0
        )
) as q;
-- Query 12c: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin(
            q1.geom::geography,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-4.5 48,
                              -2.5 47.5,
                              -2.5 46.5,
                              -4.5 47,
                              -4.5 48))'
                    )
                ,4326)::geography,
            0
        )
) as q;
-- Query 12d: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin(
            q1.geom::geography,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-4.28995878425164 48.41508842473156,
                              -4.230688452998251 48.27876666284876,
                              -4.625823994687513 47.88955815428484,
                              -5.011081147834543 48.02785559387608,
                              -4.756218723444969 48.399283003063985,
                              -4.28995878425164 48.41508842473156))'
                    )
                ,4326)::geography,
            0
        )
) as q;
-- Query 12e: Get vessels within a bounded area (geodetic dwithin)
SELECT count(distinct mmsi), count(*) 
FROM (
    SELECT * FROM ais_data.dynamic_ships as q1
    WHERE st_dwithin(
            q1.geom::geography,
            st_setsrid(
                st_geomfromtext(
                    'POLYGON((-9.765402631583392 50.950414439689965,
                              -5.65061469537787 50.84213054663192,
                              -5.677685668642381 44.263884043355986,
                              -9.792473604847903 44.18267112356246,
                              -9.765402631583392 50.950414439689965))'
                    )
                ,4326)::geography,
            0
        )
) as q;