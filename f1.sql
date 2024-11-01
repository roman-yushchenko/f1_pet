WITH
  new_points AS (
  SELECT
    CASE
      WHEN position = '1' THEN 25
      WHEN position = '2' THEN 18
      WHEN position = '3' THEN 15
      WHEN position = '4' THEN 12
      WHEN position = '5' THEN 10
      WHEN position = '6' THEN 8
      WHEN position = '7' THEN 6
      WHEN position = '8' THEN 4
      WHEN position = '9' THEN 2
      WHEN position = '10' THEN 1
      ELSE 0
  END
    AS new_points,                      -- переформатовую систему нарахування очок для діючого регламенту
    raceid,
    driverid                            -- (якщо гонщик проїхав найшвидше коло в гонці і потрапив у топ-10, то він отримує 1 бонусне очко)
  FROM
    formula-1-437007.formula_1.results ),  


  fastest_lap AS (
  SELECT
    raceid,
    driverId,
    milliseconds AS fastest_lap,
    ROW_NUMBER() OVER (PARTITION BY raceid ORDER BY milliseconds) AS lap_rank   -- визначаю найшвидше коло у кожній гонці
  FROM
    formula-1-437007.formula_1.lap_times ),


  top_10_position AS (
  SELECT
    driverId,
    raceId,
    SAFE_CAST(position AS INT64) AS position
  FROM
    formula-1-437007.formula_1.results
  WHERE
    SAFE_CAST(position AS INT64) <= 10 ),   -- визначаю топ-10 кожної гонки


  extra_point AS (
  SELECT
    CASE
      WHEN top.driverid IS NOT NULL THEN 1
      ELSE 0
  END AS extra_point, 
    top.raceId,
    top.driverid
  FROM
    fastest_lap fl
  LEFT JOIN
    top_10_position top
  ON
    fl.raceid = top.raceid
    AND fl.driverid = top.driverid
  WHERE
    fl.lap_rank = 1 )                -- Тільки пілот з найшвидшим колом і з топ10 отримує +1 бал



  SELECT
    
    drivers.driverId,
    drivers.dob AS date_of_born,
    drivers.nationality,
    COALESCE(CASE WHEN code LIKE "_N"THEN NULL ELSE code END,UPPER(SUBSTRING(driverref,1,3))) AS driver_code,  -- надаю код(зазвичай перші три літери прізвища) кожному гонщику(в деяких код відсутній)
    concat (drivers.forename, ' ', Drivers.surname) AS driver_name,

    constructors.name AS team_name,
    constructors.nationality AS team_nationality,

  
    races.name AS grand_prix_name,
    races.date AS gp_date,
    COALESCE(SAFE_CAST(results.position AS int64),20) AS position,  
    np.new_points + COALESCE(ep.extra_point, 0) AS new_total_points,           -- надаю  місце за яке не отримується очок (20) усім гонщикам що не фінішували         
    results.points,                                                              -- для можливого порівняння в майбутньому
    results.fastestLapTime,
    results.fastestLapSpeed,
    circuits.country AS gp_country,
    circuits.location AS gp_location,
    circuits.name AS track_name,
    status.status,
    results.constructorId

  FROM
    formula-1-437007.formula_1.races AS races
  LEFT JOIN
    formula-1-437007.formula_1.circuits AS circuits
  ON
    races.circuitId = circuits.circuitId
  LEFT JOIN
    formula-1-437007.formula_1.results AS results
  ON
    results.raceId = races.raceId
  LEFT JOIN
    formula-1-437007.formula_1.drivers AS drivers
  ON
    drivers.driverId = results.driverID
  LEFT JOIN
    formula-1-437007.formula_1.status AS status
  ON
    status.statusid = results.statusId
  LEFT JOIN
    extra_point ep
  ON
    results.raceid = ep.raceid
    AND ep.driverid = results.driverid
  LEFT JOIN
    new_points AS np
  ON
    results.raceid = np.raceid
    AND np.driverid = results.driverid
  LEFT JOIN 
    formula-1-437007.formula_1.constructors AS constructors
    ON 
    constructors.constructorID = results.constructorID
  WHERE
    DATE < CURRENT_DATE() -- тільки гонки які відбулися (для 07.07.2024го року)
