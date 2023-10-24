SELECT name, age, num_cars
  FROM people
  JOIN (
    SELECT owner, COUNT(*) AS num_cars
      FROM cars
      GROUP BY owner
  ) AS cars_by_owner ON people.name = cars_by_owner.owner;