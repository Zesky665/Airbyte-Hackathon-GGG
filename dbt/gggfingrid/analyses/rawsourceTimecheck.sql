USE FG_DWH
SELECT count(startTime) || ' startwind' as countcol
FROM landing.WindProductionRaw
WHERE startTime not in (SELECT endTime from landing.WindProductionRaw)
UNION 
SELECT count(endTime) || ' endWind'
FROM landing.WindProductionRaw
WHERE endTime not in (SELECT startTime from landing.WindProductionRaw)
UNION 
SELECT count(startTime) || ' startHydro'
FROM landing.HydroProductionRaw
WHERE startTime not in (SELECT endTime from landing.HydroProductionRaw)
UNION 
SELECT count(endTime) || ' endHydro'
FROM landing.HydroProductionRaw
WHERE endTime not in (SELECT startTime from landing.HydroProductionRaw)
UNION 
SELECT count(startTime) || ' startNuclear'
FROM landing.NuclearProductionRaw
WHERE startTime not in (SELECT endTime from landing.NuclearProductionRaw)
UNION 
SELECT count(endTime) || ' endNuclear'
FROM landing.NuclearProductionRaw
WHERE endTime not in (SELECT startTime from landing.HydroProductionRaw)
UNION 
SELECT count(startTime) || ' startTotal'
FROM landing.TotalElectricityProductionRaw
WHERE startTime not in (SELECT endTime from landing.TotalElectricityProductionRaw)
UNION 
SELECT count(endTime) || ' endTotal'
FROM landing.TotalElectricityProductionRaw
WHERE endTime not in (SELECT startTime from landing.TotalElectricityProductionRaw)
ORDER BY countcol