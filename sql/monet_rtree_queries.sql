-- Query R1: Intersects scalar function (multithreaded)
SELECT mmsi 
FROM dynamic_ships 
WHERE st_intersects_noindex(
        geom3035, 
        st_setsrid(
            st_geomfromtext(
                'POLYGON((2951501.933691309 3280081.578934718,
                        3228298.3396614464 3198747.5823778515,
                        3076901.3305201856 2481435.277154741,
                        2755965.824872542 2550216.8993245265,
                        2951501.933691309 3280081.578934718))'
            )
        ,3035));
-- Query R2: Intersects filter function (multithreaded)
SELECT mmsi
FROM dynamic_ships 
WHERE   [geom3035]
        st_intersects_noindex
        [st_setsrid(
            st_geomfromtext(
                'POLYGON((2951501.933691309 3280081.578934718,
                        3228298.3396614464 3198747.5823778515,
                        3076901.3305201856 2481435.277154741,
                        2755965.824872542 2550216.8993245265,
                        2951501.933691309 3280081.578934718))'
            )
        ,3035)];
-- Query R3: Intersects scalar function (singlethreaded)
SELECT mmsi 
FROM dynamic_ships 
WHERE st_intersects_noindex(
        geom3035, 
        st_setsrid(
            st_geomfromtext(
                'POLYGON((2951501.933691309 3280081.578934718,
                        3228298.3396614464 3198747.5823778515,
                        3076901.3305201856 2481435.277154741,
                        2755965.824872542 2550216.8993245265,
                        2951501.933691309 3280081.578934718))'
            )
        ,3035));
-- Query R4: Intersects filter function (singlethreaded)
SELECT mmsi
FROM dynamic_ships 
WHERE   [geom3035]
        st_intersects_noindex
        [st_setsrid(
            st_geomfromtext(
                'POLYGON((2951501.933691309 3280081.578934718,
                        3228298.3396614464 3198747.5823778515,
                        3076901.3305201856 2481435.277154741,
                        2755965.824872542 2550216.8993245265,
                        2951501.933691309 3280081.578934718))'
            )
        ,3035)];
-- Query R5: Intersects R-Tree filter function (singlethreaded)
SELECT mmsi
FROM dynamic_ships 
WHERE   [geom3035]
        st_intersects
        [st_setsrid(
            st_geomfromtext(
                'POLYGON((2951501.933691309 3280081.578934718,
                        3228298.3396614464 3198747.5823778515,
                        3076901.3305201856 2481435.277154741,
                        2755965.824872542 2550216.8993245265,
                        2951501.933691309 3280081.578934718))'
            )
        ,3035)];
-- Query R6: Intersects scalar function (multithreaded)
SELECT mmsi 
FROM dynamic_ships 
WHERE st_intersects_noindex(
        geom3035, 
        st_setsrid(
            st_geomfromtext(
                'POLYGON ((3206419.745465245 2960822.791605039, 3192068.37367636 2832982.329905018, 3221416.9265468945 2818248.0513150496, 3230554.401666465 2827350.535852826, 3230398.497539357 2815658.545243084, 3241915.7442081748 2818050.133579226, 3241359.97962444 2804100.531118586, 3236878.100332672 2772187.5516476477, 3229578.990409313 2753328.9475566526, 3205413.973610296 2719098.385636027, 3222002.985842135 2704863.0045618815, 3219163.980717857 2689813.5350874593, 3200473.270959447 2684942.345458572, 3189268.009508293 2691106.52045757, 3200599.232247385 2705224.3354333714, 3180365.570995475 2711680.276268403, 3176242.272192748 2690635.7757973364, 3184036.187795396 2664040.9788565994, 3183095.9340218613 2633770.921202909, 3169866.8041689303 2635670.835184064, 3157753.8510894002 2640499.151357119, 3161807.2008198723 2663897.77616952, 3136071.690053747 2665360.98385505, 3129750.148227185 2639304.878735087, 3115293.1805418115 2648642.2421258516, 3132059.666442721 2666992.5603574943, 3144887.2719110716 2682286.2608149117, 3112433.54553707 2679807.995222575, 3087388.389657242 2658612.835906004, 3090218.0917740976 2640746.0638562264, 3061806.3432709575 2614857.1252443646, 3049557.669117264 2627011.7702896106, 3040820.3911633184 2639979.0920885378, 3059569.4254984884 2648310.8888708726, 3064548.4845731044 2633074.038440468, 3077265.121634067 2646726.8917796286, 3073599.3776313984 2665582.4191441494, 3051994.2561909733 2666505.0472804657, 3047933.3340856982 2650146.6358738886, 3026858.227739764 2658889.010549373, 3029320.077617931 2685053.4849079475, 3056648.3728655465 2678018.4977814993, 3075616.499532461 2677688.917343071, 3081897.0544004594 2712393.877941596, 3057443.1061685095 2694333.742480806, 3045219.1971002044 2710482.661542364, 3064651.200125831 2720197.4812862324, 3085037.840407366 2719543.6694709463, 3099302.12991435 2736000.099669498, 3083671.5007202025 2755971.0996459587, 3072859.7983775362 2748196.3746611797, 3065184.8105073227 2732648.5873780283, 3057147.058265354 2732907.459825224, 3043600.353423765 2717931.519757656, 3019904.325270051 2723442.297736103, 3015391.86594895 2703247.291094465, 2997064.205316324 2687101.127850837, 2996232.699701455 2731420.5395200993, 3012635.441921711 2744058.7904671747, 3030070.419312945 2742314.456927762, 3053034.7729813475 2753508.719968442, 3075104.1535356487 2761047.156281307, 3089053.309954699 2761047.202516262, 3104995.815309413 2752791.900683832, 3116447.1238593766 2740865.764739177, 3136847.328660044 2751345.707104465, 3127978.9891306194 2770508.842222798, 3111000.4631021153 2771086.87094756, 3100019.7174527124 2767237.9532143986, 3088776.873673268 2769749.2672781544, 3068860.9687756468 2774258.180532325, 3060037.089355631 2773134.2798781097, 3056728.229657544 2760524.071391537, 3028110.4800939616 2762457.9530401775, 3008365.944553163 2782120.7128537, 3019955.6591669256 2809308.044586128, 3043125.838708274 2803033.658418214, 3038928.922411551 2784329.8884457033, 3052841.5315045537 2784236.32452193, 3067868.799052821 2794145.649655231, 3086928.3367293123 2783521.0660384195, 3102893.0378137957 2780731.833725718, 3117736.5113794273 2785299.3662944827, 3124753.5221434915 2780623.507738383, 3139671.5884412485 2808752.477111797, 3122172.125834184 2818857.558584719, 3100971.2724472135 2816506.4749410367, 3097719.495697427 2803879.5020756386, 3089865.6611827617 2794645.709738425, 3064580.4053300247 2808276.571117348, 3057328.6488295095 2804445.416511223, 3043451.236177624 2818710.462093517, 3028432.9357694574 2820688.360969748, 3015229.420837739 2812017.614493687, 3006843.257473043 2824296.4624411575, 3024088.8980267104 2861952.9629777307, 3057260.347903254 2844616.153840893, 3085995.9484517896 2840335.8672676664, 3084217.457926337 2875363.465213319, 3058966.8572398447 2868636.7828801116, 3055093.4027027916 2858516.1487196367, 3026668.0529679605 2870800.576983811, 3026003.887716316 2890692.6692737085, 3036859.4168830467 2910171.8856602763, 3072403.5662900936 2900154.3407535176, 3071873.146351639 2888461.7628369974, 3095387.8359207446 2883029.6400804194, 3111155.941527494 2866085.318435326, 3117593.142673821 2837146.3241926935, 3128198.2128742654 2834799.881738208, 3144560.5935747707 2829654.0808393275, 3153868.774695409 2815883.2808573553, 3185099.5837030094 2842164.0685592145, 3158488.229127041 2850957.3988093347, 3134588.2494434416 2847524.460096912, 3122709.982103613 2863500.2832381944, 3113730.314577102 2882799.4306159355, 3096709.2611804605 2889021.806856256, 3101179.101040152 2904523.1850753212, 3086225.0736546256 2907964.480293633, 3063776.370699042 2906120.2930970835, 3058256.1891229227 2925562.2660488887, 3048606.5040156743 2922343.175218528, 3036712.172048926 2940998.5691461125, 3067426.2850950584 2969920.595886969, 3089494.1650237776 2945799.1278489158, 3112117.7958182674 2995692.028063138, 3044302.521314625 3038718.741819533, 3075121.7249939404 3060440.098733634, 3113745.4740592437 3037072.6647254396, 3125608.5434028925 3012273.9125991506, 3171710.8406304293 3030990.9378971993, 3168003.819146199 2968172.8991557565, 3213653.656789554 3021887.3277138304, 3259023.22464741 3017139.4400814534, 3272422.544143065 2981578.395423152, 3262799.057767988 2958480.2645381265, 3253223.2182402215 2946328.2021011803, 3229272.684994609 2934775.7469022395, 3239442.172232068 3000801.6657612217, 3208252.71983575 2990880.957546086, 3177764.78868914 2940931.906821911, 3206419.745465245 2960822.791605039))'
            )
        ,3035));
-- Query R7: Intersects filter function (multithreaded)
SELECT mmsi
FROM dynamic_ships 
WHERE   [geom3035]
        st_intersects_noindex
        [st_setsrid(
            st_geomfromtext(
                'POLYGON ((3206419.745465245 2960822.791605039, 3192068.37367636 2832982.329905018, 3221416.9265468945 2818248.0513150496, 3230554.401666465 2827350.535852826, 3230398.497539357 2815658.545243084, 3241915.7442081748 2818050.133579226, 3241359.97962444 2804100.531118586, 3236878.100332672 2772187.5516476477, 3229578.990409313 2753328.9475566526, 3205413.973610296 2719098.385636027, 3222002.985842135 2704863.0045618815, 3219163.980717857 2689813.5350874593, 3200473.270959447 2684942.345458572, 3189268.009508293 2691106.52045757, 3200599.232247385 2705224.3354333714, 3180365.570995475 2711680.276268403, 3176242.272192748 2690635.7757973364, 3184036.187795396 2664040.9788565994, 3183095.9340218613 2633770.921202909, 3169866.8041689303 2635670.835184064, 3157753.8510894002 2640499.151357119, 3161807.2008198723 2663897.77616952, 3136071.690053747 2665360.98385505, 3129750.148227185 2639304.878735087, 3115293.1805418115 2648642.2421258516, 3132059.666442721 2666992.5603574943, 3144887.2719110716 2682286.2608149117, 3112433.54553707 2679807.995222575, 3087388.389657242 2658612.835906004, 3090218.0917740976 2640746.0638562264, 3061806.3432709575 2614857.1252443646, 3049557.669117264 2627011.7702896106, 3040820.3911633184 2639979.0920885378, 3059569.4254984884 2648310.8888708726, 3064548.4845731044 2633074.038440468, 3077265.121634067 2646726.8917796286, 3073599.3776313984 2665582.4191441494, 3051994.2561909733 2666505.0472804657, 3047933.3340856982 2650146.6358738886, 3026858.227739764 2658889.010549373, 3029320.077617931 2685053.4849079475, 3056648.3728655465 2678018.4977814993, 3075616.499532461 2677688.917343071, 3081897.0544004594 2712393.877941596, 3057443.1061685095 2694333.742480806, 3045219.1971002044 2710482.661542364, 3064651.200125831 2720197.4812862324, 3085037.840407366 2719543.6694709463, 3099302.12991435 2736000.099669498, 3083671.5007202025 2755971.0996459587, 3072859.7983775362 2748196.3746611797, 3065184.8105073227 2732648.5873780283, 3057147.058265354 2732907.459825224, 3043600.353423765 2717931.519757656, 3019904.325270051 2723442.297736103, 3015391.86594895 2703247.291094465, 2997064.205316324 2687101.127850837, 2996232.699701455 2731420.5395200993, 3012635.441921711 2744058.7904671747, 3030070.419312945 2742314.456927762, 3053034.7729813475 2753508.719968442, 3075104.1535356487 2761047.156281307, 3089053.309954699 2761047.202516262, 3104995.815309413 2752791.900683832, 3116447.1238593766 2740865.764739177, 3136847.328660044 2751345.707104465, 3127978.9891306194 2770508.842222798, 3111000.4631021153 2771086.87094756, 3100019.7174527124 2767237.9532143986, 3088776.873673268 2769749.2672781544, 3068860.9687756468 2774258.180532325, 3060037.089355631 2773134.2798781097, 3056728.229657544 2760524.071391537, 3028110.4800939616 2762457.9530401775, 3008365.944553163 2782120.7128537, 3019955.6591669256 2809308.044586128, 3043125.838708274 2803033.658418214, 3038928.922411551 2784329.8884457033, 3052841.5315045537 2784236.32452193, 3067868.799052821 2794145.649655231, 3086928.3367293123 2783521.0660384195, 3102893.0378137957 2780731.833725718, 3117736.5113794273 2785299.3662944827, 3124753.5221434915 2780623.507738383, 3139671.5884412485 2808752.477111797, 3122172.125834184 2818857.558584719, 3100971.2724472135 2816506.4749410367, 3097719.495697427 2803879.5020756386, 3089865.6611827617 2794645.709738425, 3064580.4053300247 2808276.571117348, 3057328.6488295095 2804445.416511223, 3043451.236177624 2818710.462093517, 3028432.9357694574 2820688.360969748, 3015229.420837739 2812017.614493687, 3006843.257473043 2824296.4624411575, 3024088.8980267104 2861952.9629777307, 3057260.347903254 2844616.153840893, 3085995.9484517896 2840335.8672676664, 3084217.457926337 2875363.465213319, 3058966.8572398447 2868636.7828801116, 3055093.4027027916 2858516.1487196367, 3026668.0529679605 2870800.576983811, 3026003.887716316 2890692.6692737085, 3036859.4168830467 2910171.8856602763, 3072403.5662900936 2900154.3407535176, 3071873.146351639 2888461.7628369974, 3095387.8359207446 2883029.6400804194, 3111155.941527494 2866085.318435326, 3117593.142673821 2837146.3241926935, 3128198.2128742654 2834799.881738208, 3144560.5935747707 2829654.0808393275, 3153868.774695409 2815883.2808573553, 3185099.5837030094 2842164.0685592145, 3158488.229127041 2850957.3988093347, 3134588.2494434416 2847524.460096912, 3122709.982103613 2863500.2832381944, 3113730.314577102 2882799.4306159355, 3096709.2611804605 2889021.806856256, 3101179.101040152 2904523.1850753212, 3086225.0736546256 2907964.480293633, 3063776.370699042 2906120.2930970835, 3058256.1891229227 2925562.2660488887, 3048606.5040156743 2922343.175218528, 3036712.172048926 2940998.5691461125, 3067426.2850950584 2969920.595886969, 3089494.1650237776 2945799.1278489158, 3112117.7958182674 2995692.028063138, 3044302.521314625 3038718.741819533, 3075121.7249939404 3060440.098733634, 3113745.4740592437 3037072.6647254396, 3125608.5434028925 3012273.9125991506, 3171710.8406304293 3030990.9378971993, 3168003.819146199 2968172.8991557565, 3213653.656789554 3021887.3277138304, 3259023.22464741 3017139.4400814534, 3272422.544143065 2981578.395423152, 3262799.057767988 2958480.2645381265, 3253223.2182402215 2946328.2021011803, 3229272.684994609 2934775.7469022395, 3239442.172232068 3000801.6657612217, 3208252.71983575 2990880.957546086, 3177764.78868914 2940931.906821911, 3206419.745465245 2960822.791605039))'
            )
        ,3035)];
-- Query R8: Intersects scalar function (singlethreaded)
SELECT mmsi 
FROM dynamic_ships 
WHERE st_intersects_noindex(
        geom3035, 
        st_setsrid(
            st_geomfromtext(
                'POLYGON ((3206419.745465245 2960822.791605039, 3192068.37367636 2832982.329905018, 3221416.9265468945 2818248.0513150496, 3230554.401666465 2827350.535852826, 3230398.497539357 2815658.545243084, 3241915.7442081748 2818050.133579226, 3241359.97962444 2804100.531118586, 3236878.100332672 2772187.5516476477, 3229578.990409313 2753328.9475566526, 3205413.973610296 2719098.385636027, 3222002.985842135 2704863.0045618815, 3219163.980717857 2689813.5350874593, 3200473.270959447 2684942.345458572, 3189268.009508293 2691106.52045757, 3200599.232247385 2705224.3354333714, 3180365.570995475 2711680.276268403, 3176242.272192748 2690635.7757973364, 3184036.187795396 2664040.9788565994, 3183095.9340218613 2633770.921202909, 3169866.8041689303 2635670.835184064, 3157753.8510894002 2640499.151357119, 3161807.2008198723 2663897.77616952, 3136071.690053747 2665360.98385505, 3129750.148227185 2639304.878735087, 3115293.1805418115 2648642.2421258516, 3132059.666442721 2666992.5603574943, 3144887.2719110716 2682286.2608149117, 3112433.54553707 2679807.995222575, 3087388.389657242 2658612.835906004, 3090218.0917740976 2640746.0638562264, 3061806.3432709575 2614857.1252443646, 3049557.669117264 2627011.7702896106, 3040820.3911633184 2639979.0920885378, 3059569.4254984884 2648310.8888708726, 3064548.4845731044 2633074.038440468, 3077265.121634067 2646726.8917796286, 3073599.3776313984 2665582.4191441494, 3051994.2561909733 2666505.0472804657, 3047933.3340856982 2650146.6358738886, 3026858.227739764 2658889.010549373, 3029320.077617931 2685053.4849079475, 3056648.3728655465 2678018.4977814993, 3075616.499532461 2677688.917343071, 3081897.0544004594 2712393.877941596, 3057443.1061685095 2694333.742480806, 3045219.1971002044 2710482.661542364, 3064651.200125831 2720197.4812862324, 3085037.840407366 2719543.6694709463, 3099302.12991435 2736000.099669498, 3083671.5007202025 2755971.0996459587, 3072859.7983775362 2748196.3746611797, 3065184.8105073227 2732648.5873780283, 3057147.058265354 2732907.459825224, 3043600.353423765 2717931.519757656, 3019904.325270051 2723442.297736103, 3015391.86594895 2703247.291094465, 2997064.205316324 2687101.127850837, 2996232.699701455 2731420.5395200993, 3012635.441921711 2744058.7904671747, 3030070.419312945 2742314.456927762, 3053034.7729813475 2753508.719968442, 3075104.1535356487 2761047.156281307, 3089053.309954699 2761047.202516262, 3104995.815309413 2752791.900683832, 3116447.1238593766 2740865.764739177, 3136847.328660044 2751345.707104465, 3127978.9891306194 2770508.842222798, 3111000.4631021153 2771086.87094756, 3100019.7174527124 2767237.9532143986, 3088776.873673268 2769749.2672781544, 3068860.9687756468 2774258.180532325, 3060037.089355631 2773134.2798781097, 3056728.229657544 2760524.071391537, 3028110.4800939616 2762457.9530401775, 3008365.944553163 2782120.7128537, 3019955.6591669256 2809308.044586128, 3043125.838708274 2803033.658418214, 3038928.922411551 2784329.8884457033, 3052841.5315045537 2784236.32452193, 3067868.799052821 2794145.649655231, 3086928.3367293123 2783521.0660384195, 3102893.0378137957 2780731.833725718, 3117736.5113794273 2785299.3662944827, 3124753.5221434915 2780623.507738383, 3139671.5884412485 2808752.477111797, 3122172.125834184 2818857.558584719, 3100971.2724472135 2816506.4749410367, 3097719.495697427 2803879.5020756386, 3089865.6611827617 2794645.709738425, 3064580.4053300247 2808276.571117348, 3057328.6488295095 2804445.416511223, 3043451.236177624 2818710.462093517, 3028432.9357694574 2820688.360969748, 3015229.420837739 2812017.614493687, 3006843.257473043 2824296.4624411575, 3024088.8980267104 2861952.9629777307, 3057260.347903254 2844616.153840893, 3085995.9484517896 2840335.8672676664, 3084217.457926337 2875363.465213319, 3058966.8572398447 2868636.7828801116, 3055093.4027027916 2858516.1487196367, 3026668.0529679605 2870800.576983811, 3026003.887716316 2890692.6692737085, 3036859.4168830467 2910171.8856602763, 3072403.5662900936 2900154.3407535176, 3071873.146351639 2888461.7628369974, 3095387.8359207446 2883029.6400804194, 3111155.941527494 2866085.318435326, 3117593.142673821 2837146.3241926935, 3128198.2128742654 2834799.881738208, 3144560.5935747707 2829654.0808393275, 3153868.774695409 2815883.2808573553, 3185099.5837030094 2842164.0685592145, 3158488.229127041 2850957.3988093347, 3134588.2494434416 2847524.460096912, 3122709.982103613 2863500.2832381944, 3113730.314577102 2882799.4306159355, 3096709.2611804605 2889021.806856256, 3101179.101040152 2904523.1850753212, 3086225.0736546256 2907964.480293633, 3063776.370699042 2906120.2930970835, 3058256.1891229227 2925562.2660488887, 3048606.5040156743 2922343.175218528, 3036712.172048926 2940998.5691461125, 3067426.2850950584 2969920.595886969, 3089494.1650237776 2945799.1278489158, 3112117.7958182674 2995692.028063138, 3044302.521314625 3038718.741819533, 3075121.7249939404 3060440.098733634, 3113745.4740592437 3037072.6647254396, 3125608.5434028925 3012273.9125991506, 3171710.8406304293 3030990.9378971993, 3168003.819146199 2968172.8991557565, 3213653.656789554 3021887.3277138304, 3259023.22464741 3017139.4400814534, 3272422.544143065 2981578.395423152, 3262799.057767988 2958480.2645381265, 3253223.2182402215 2946328.2021011803, 3229272.684994609 2934775.7469022395, 3239442.172232068 3000801.6657612217, 3208252.71983575 2990880.957546086, 3177764.78868914 2940931.906821911, 3206419.745465245 2960822.791605039))'
            )
        ,3035));
-- Query R9: Intersects filter function (singlethreaded)
SELECT mmsi
FROM dynamic_ships 
WHERE   [geom3035]
        st_intersects_noindex
        [st_setsrid(
            st_geomfromtext(
                'POLYGON ((3206419.745465245 2960822.791605039, 3192068.37367636 2832982.329905018, 3221416.9265468945 2818248.0513150496, 3230554.401666465 2827350.535852826, 3230398.497539357 2815658.545243084, 3241915.7442081748 2818050.133579226, 3241359.97962444 2804100.531118586, 3236878.100332672 2772187.5516476477, 3229578.990409313 2753328.9475566526, 3205413.973610296 2719098.385636027, 3222002.985842135 2704863.0045618815, 3219163.980717857 2689813.5350874593, 3200473.270959447 2684942.345458572, 3189268.009508293 2691106.52045757, 3200599.232247385 2705224.3354333714, 3180365.570995475 2711680.276268403, 3176242.272192748 2690635.7757973364, 3184036.187795396 2664040.9788565994, 3183095.9340218613 2633770.921202909, 3169866.8041689303 2635670.835184064, 3157753.8510894002 2640499.151357119, 3161807.2008198723 2663897.77616952, 3136071.690053747 2665360.98385505, 3129750.148227185 2639304.878735087, 3115293.1805418115 2648642.2421258516, 3132059.666442721 2666992.5603574943, 3144887.2719110716 2682286.2608149117, 3112433.54553707 2679807.995222575, 3087388.389657242 2658612.835906004, 3090218.0917740976 2640746.0638562264, 3061806.3432709575 2614857.1252443646, 3049557.669117264 2627011.7702896106, 3040820.3911633184 2639979.0920885378, 3059569.4254984884 2648310.8888708726, 3064548.4845731044 2633074.038440468, 3077265.121634067 2646726.8917796286, 3073599.3776313984 2665582.4191441494, 3051994.2561909733 2666505.0472804657, 3047933.3340856982 2650146.6358738886, 3026858.227739764 2658889.010549373, 3029320.077617931 2685053.4849079475, 3056648.3728655465 2678018.4977814993, 3075616.499532461 2677688.917343071, 3081897.0544004594 2712393.877941596, 3057443.1061685095 2694333.742480806, 3045219.1971002044 2710482.661542364, 3064651.200125831 2720197.4812862324, 3085037.840407366 2719543.6694709463, 3099302.12991435 2736000.099669498, 3083671.5007202025 2755971.0996459587, 3072859.7983775362 2748196.3746611797, 3065184.8105073227 2732648.5873780283, 3057147.058265354 2732907.459825224, 3043600.353423765 2717931.519757656, 3019904.325270051 2723442.297736103, 3015391.86594895 2703247.291094465, 2997064.205316324 2687101.127850837, 2996232.699701455 2731420.5395200993, 3012635.441921711 2744058.7904671747, 3030070.419312945 2742314.456927762, 3053034.7729813475 2753508.719968442, 3075104.1535356487 2761047.156281307, 3089053.309954699 2761047.202516262, 3104995.815309413 2752791.900683832, 3116447.1238593766 2740865.764739177, 3136847.328660044 2751345.707104465, 3127978.9891306194 2770508.842222798, 3111000.4631021153 2771086.87094756, 3100019.7174527124 2767237.9532143986, 3088776.873673268 2769749.2672781544, 3068860.9687756468 2774258.180532325, 3060037.089355631 2773134.2798781097, 3056728.229657544 2760524.071391537, 3028110.4800939616 2762457.9530401775, 3008365.944553163 2782120.7128537, 3019955.6591669256 2809308.044586128, 3043125.838708274 2803033.658418214, 3038928.922411551 2784329.8884457033, 3052841.5315045537 2784236.32452193, 3067868.799052821 2794145.649655231, 3086928.3367293123 2783521.0660384195, 3102893.0378137957 2780731.833725718, 3117736.5113794273 2785299.3662944827, 3124753.5221434915 2780623.507738383, 3139671.5884412485 2808752.477111797, 3122172.125834184 2818857.558584719, 3100971.2724472135 2816506.4749410367, 3097719.495697427 2803879.5020756386, 3089865.6611827617 2794645.709738425, 3064580.4053300247 2808276.571117348, 3057328.6488295095 2804445.416511223, 3043451.236177624 2818710.462093517, 3028432.9357694574 2820688.360969748, 3015229.420837739 2812017.614493687, 3006843.257473043 2824296.4624411575, 3024088.8980267104 2861952.9629777307, 3057260.347903254 2844616.153840893, 3085995.9484517896 2840335.8672676664, 3084217.457926337 2875363.465213319, 3058966.8572398447 2868636.7828801116, 3055093.4027027916 2858516.1487196367, 3026668.0529679605 2870800.576983811, 3026003.887716316 2890692.6692737085, 3036859.4168830467 2910171.8856602763, 3072403.5662900936 2900154.3407535176, 3071873.146351639 2888461.7628369974, 3095387.8359207446 2883029.6400804194, 3111155.941527494 2866085.318435326, 3117593.142673821 2837146.3241926935, 3128198.2128742654 2834799.881738208, 3144560.5935747707 2829654.0808393275, 3153868.774695409 2815883.2808573553, 3185099.5837030094 2842164.0685592145, 3158488.229127041 2850957.3988093347, 3134588.2494434416 2847524.460096912, 3122709.982103613 2863500.2832381944, 3113730.314577102 2882799.4306159355, 3096709.2611804605 2889021.806856256, 3101179.101040152 2904523.1850753212, 3086225.0736546256 2907964.480293633, 3063776.370699042 2906120.2930970835, 3058256.1891229227 2925562.2660488887, 3048606.5040156743 2922343.175218528, 3036712.172048926 2940998.5691461125, 3067426.2850950584 2969920.595886969, 3089494.1650237776 2945799.1278489158, 3112117.7958182674 2995692.028063138, 3044302.521314625 3038718.741819533, 3075121.7249939404 3060440.098733634, 3113745.4740592437 3037072.6647254396, 3125608.5434028925 3012273.9125991506, 3171710.8406304293 3030990.9378971993, 3168003.819146199 2968172.8991557565, 3213653.656789554 3021887.3277138304, 3259023.22464741 3017139.4400814534, 3272422.544143065 2981578.395423152, 3262799.057767988 2958480.2645381265, 3253223.2182402215 2946328.2021011803, 3229272.684994609 2934775.7469022395, 3239442.172232068 3000801.6657612217, 3208252.71983575 2990880.957546086, 3177764.78868914 2940931.906821911, 3206419.745465245 2960822.791605039))'
            )
        ,3035)];
-- Query R10: Intersects R-Tree filter function (singlethreaded)
SELECT mmsi
FROM dynamic_ships 
WHERE   [geom3035]
        st_intersects
        [st_setsrid(
            st_geomfromtext(
                'POLYGON ((3206419.745465245 2960822.791605039, 3192068.37367636 2832982.329905018, 3221416.9265468945 2818248.0513150496, 3230554.401666465 2827350.535852826, 3230398.497539357 2815658.545243084, 3241915.7442081748 2818050.133579226, 3241359.97962444 2804100.531118586, 3236878.100332672 2772187.5516476477, 3229578.990409313 2753328.9475566526, 3205413.973610296 2719098.385636027, 3222002.985842135 2704863.0045618815, 3219163.980717857 2689813.5350874593, 3200473.270959447 2684942.345458572, 3189268.009508293 2691106.52045757, 3200599.232247385 2705224.3354333714, 3180365.570995475 2711680.276268403, 3176242.272192748 2690635.7757973364, 3184036.187795396 2664040.9788565994, 3183095.9340218613 2633770.921202909, 3169866.8041689303 2635670.835184064, 3157753.8510894002 2640499.151357119, 3161807.2008198723 2663897.77616952, 3136071.690053747 2665360.98385505, 3129750.148227185 2639304.878735087, 3115293.1805418115 2648642.2421258516, 3132059.666442721 2666992.5603574943, 3144887.2719110716 2682286.2608149117, 3112433.54553707 2679807.995222575, 3087388.389657242 2658612.835906004, 3090218.0917740976 2640746.0638562264, 3061806.3432709575 2614857.1252443646, 3049557.669117264 2627011.7702896106, 3040820.3911633184 2639979.0920885378, 3059569.4254984884 2648310.8888708726, 3064548.4845731044 2633074.038440468, 3077265.121634067 2646726.8917796286, 3073599.3776313984 2665582.4191441494, 3051994.2561909733 2666505.0472804657, 3047933.3340856982 2650146.6358738886, 3026858.227739764 2658889.010549373, 3029320.077617931 2685053.4849079475, 3056648.3728655465 2678018.4977814993, 3075616.499532461 2677688.917343071, 3081897.0544004594 2712393.877941596, 3057443.1061685095 2694333.742480806, 3045219.1971002044 2710482.661542364, 3064651.200125831 2720197.4812862324, 3085037.840407366 2719543.6694709463, 3099302.12991435 2736000.099669498, 3083671.5007202025 2755971.0996459587, 3072859.7983775362 2748196.3746611797, 3065184.8105073227 2732648.5873780283, 3057147.058265354 2732907.459825224, 3043600.353423765 2717931.519757656, 3019904.325270051 2723442.297736103, 3015391.86594895 2703247.291094465, 2997064.205316324 2687101.127850837, 2996232.699701455 2731420.5395200993, 3012635.441921711 2744058.7904671747, 3030070.419312945 2742314.456927762, 3053034.7729813475 2753508.719968442, 3075104.1535356487 2761047.156281307, 3089053.309954699 2761047.202516262, 3104995.815309413 2752791.900683832, 3116447.1238593766 2740865.764739177, 3136847.328660044 2751345.707104465, 3127978.9891306194 2770508.842222798, 3111000.4631021153 2771086.87094756, 3100019.7174527124 2767237.9532143986, 3088776.873673268 2769749.2672781544, 3068860.9687756468 2774258.180532325, 3060037.089355631 2773134.2798781097, 3056728.229657544 2760524.071391537, 3028110.4800939616 2762457.9530401775, 3008365.944553163 2782120.7128537, 3019955.6591669256 2809308.044586128, 3043125.838708274 2803033.658418214, 3038928.922411551 2784329.8884457033, 3052841.5315045537 2784236.32452193, 3067868.799052821 2794145.649655231, 3086928.3367293123 2783521.0660384195, 3102893.0378137957 2780731.833725718, 3117736.5113794273 2785299.3662944827, 3124753.5221434915 2780623.507738383, 3139671.5884412485 2808752.477111797, 3122172.125834184 2818857.558584719, 3100971.2724472135 2816506.4749410367, 3097719.495697427 2803879.5020756386, 3089865.6611827617 2794645.709738425, 3064580.4053300247 2808276.571117348, 3057328.6488295095 2804445.416511223, 3043451.236177624 2818710.462093517, 3028432.9357694574 2820688.360969748, 3015229.420837739 2812017.614493687, 3006843.257473043 2824296.4624411575, 3024088.8980267104 2861952.9629777307, 3057260.347903254 2844616.153840893, 3085995.9484517896 2840335.8672676664, 3084217.457926337 2875363.465213319, 3058966.8572398447 2868636.7828801116, 3055093.4027027916 2858516.1487196367, 3026668.0529679605 2870800.576983811, 3026003.887716316 2890692.6692737085, 3036859.4168830467 2910171.8856602763, 3072403.5662900936 2900154.3407535176, 3071873.146351639 2888461.7628369974, 3095387.8359207446 2883029.6400804194, 3111155.941527494 2866085.318435326, 3117593.142673821 2837146.3241926935, 3128198.2128742654 2834799.881738208, 3144560.5935747707 2829654.0808393275, 3153868.774695409 2815883.2808573553, 3185099.5837030094 2842164.0685592145, 3158488.229127041 2850957.3988093347, 3134588.2494434416 2847524.460096912, 3122709.982103613 2863500.2832381944, 3113730.314577102 2882799.4306159355, 3096709.2611804605 2889021.806856256, 3101179.101040152 2904523.1850753212, 3086225.0736546256 2907964.480293633, 3063776.370699042 2906120.2930970835, 3058256.1891229227 2925562.2660488887, 3048606.5040156743 2922343.175218528, 3036712.172048926 2940998.5691461125, 3067426.2850950584 2969920.595886969, 3089494.1650237776 2945799.1278489158, 3112117.7958182674 2995692.028063138, 3044302.521314625 3038718.741819533, 3075121.7249939404 3060440.098733634, 3113745.4740592437 3037072.6647254396, 3125608.5434028925 3012273.9125991506, 3171710.8406304293 3030990.9378971993, 3168003.819146199 2968172.8991557565, 3213653.656789554 3021887.3277138304, 3259023.22464741 3017139.4400814534, 3272422.544143065 2981578.395423152, 3262799.057767988 2958480.2645381265, 3253223.2182402215 2946328.2021011803, 3229272.684994609 2934775.7469022395, 3239442.172232068 3000801.6657612217, 3208252.71983575 2990880.957546086, 3177764.78868914 2940931.906821911, 3206419.745465245 2960822.791605039))'
            )
        ,3035)];