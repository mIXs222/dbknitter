CREATE DATABASE pulp;
USE pulp;

CREATE TABLE cars (owner CHAR(100) NOT NULL, plate_id INTEGER NOT NULL);
INSERT INTO cars
  (owner, plate_id)
  VALUES
  ("Vincent Vega", 101),
  ("Jules", 201),
  ("Mia", 301),
  ("Marsellus", 401),
  ("Marsellus", 402),
  ("Marsellus", 403),
  ("Marsellus", 404),
  ("Marsellus", 405),
  ("Mr. Wolf", 501);