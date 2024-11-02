Я вже значний час слідкую за авто та мотоперегонами, і звісно мою увагу не обійшла і "королева" автоперегонів , а саме Формула 1. Коли почав знайомитися з Power BI
, то ідея з'явилася сама собою, а коли в попшуках цікавих датиасетів на Kaggle випадково знайшов результати всіх гонок за всі роки (включно на 07/07/2024), то пазл склався, і ідея зробити дашборд по Ф1 перестала бути просто ідеєю.

 Для початку я хотів зібрати всі необхідні дані в одному файлі (так як датасет був "розкиданий" по різним файлам, для мене це було не дуже зручно) , тут знвдобилися знання SQL. Аля я не я якщо не вигадаю собі заняття доки виконую заняття, виникло питання "а що, якщо очки нараховувалися за однією системою весь час?" 

* спочатку перероблюю систему нарахкування очок. Очки протягом історії нараховувалися по різному, проте діючий регламент нарахування очок наступний : 1 місце - 25 очок, 2 - 18, 3 - 15  і тд.
  частина запиту яка відповідає за форматування очок під один регламент 
``` javascript
 -- переформатовую систему нарахування очок для діючого регламенту 

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
    AS new_points,                     
    raceid,
    driverid                            
  FROM
    formula-1-437007.formula_1.results ),
```


* також в діючому регламенті передбачено нарахування додаткового extra-point, для отримання якого гонщику потрібно проїхати найшвидше коло в гонці і фінішувати в топ-10

``` javascript
-- визначаю найшвидше коло у кожній гонці

  fastest_lap AS (
  SELECT
    raceid,
    driverId,
    milliseconds AS fastest_lap,
    ROW_NUMBER() OVER (PARTITION BY raceid ORDER BY milliseconds) AS lap_rank   
    formula-1-437007.formula_1.lap_times ),


 -- визначаю топ-10 кожної гонки

  top_10_position AS (
  SELECT
    driverId,
    raceId,
    SAFE_CAST(position AS INT64) AS position
  FROM
    formula-1-437007.formula_1.results
  WHERE
    SAFE_CAST(position AS INT64) <= 10 ),  


 -- визначаю пілота  з найшвидшим колом і з топ10 

  extra_point AS (
  SELECT
    CASE
      WHEN top.driverid IS NOT NULL THEN 1
      ELSE 0
  END AS extra_point, 
    top.raceId,
    top.driverid
  FROM
    fastest_lap AS fl
  LEFT JOIN
    top_10_position AS top
  ON
    fl.raceid = top.raceid
    AND fl.driverid = top.driverid
  WHERE
    fl.lap_rank = 1 )               
  
  ```

і тепер можна перейти до основного запиту, який збере всі необхідні для мого проекту дані в одному файлі

``` javascript

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
    COALESCE(SAFE_CAST(results.position AS int64),20) AS position,  -- надаю  місце за яке не отримується очок (20) усім гонщикам що не фінішували 
    np.new_points + COALESCE(ep.extra_point, 0) AS new_total_points,       -- рахую очки які б отримали гощики за новою системою включно з extra-point            
    results.points,                                                              
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
    DATE < CURRENT_DATE() 
```

В результаті я отримав таблицю formula1_all_grand_prix.csv. 

Наступним кроком був імпорт в Power BI, для побудови дашборду.





