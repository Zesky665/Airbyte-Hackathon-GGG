SELECT 
endTime::DATE as GeneratedAtDate,
endTime::TIME as GeneratedAtTime, 
"Wind power production - real time data" as WindPowerGenerated,
"Nuclear power production - real time data" as NuclearPowerGenerated,
"Hydro power production - real time data" as HydroPowerGenerated,
"Electricity production in Finland - real time data" as AllPowerGenerated
FROM {{ ref('src_fingrid_model') }}