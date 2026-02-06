
//CREATE TABLE `orc_nested_type` (
//  `c1` int,
//  `c2` varchar(60),
//  `c3` string,
//  `c4` bigint,
//  `c5` char(40),
//  `c6` float,
//  `c7` double,
//  `c8` decimal(9,8),
//  `c9` decimal(18,5),
//  `c10` boolean,
//  `c11` smallint,
//  `c12` timestamp,
//  `c13` date,
//  -- New nested type fields
//  `c14` array<int>,  -- Integer array
//  `c15` array<string>,  -- String array
//  `c16` struct<address:string,city:string,zip:int>,  -- Struct type
//  `c17` map<string,double>,  -- Map from string to double
//  `c18` array<struct<name:string,value:float>>  -- Array of structs
//) stored as orc;
//
//-- 1st row
//INSERT INTO orc_nested_test VALUES (
//  1,                                  -- c1 int
//  'varchar value 1',                  -- c2 varchar(60)
//  'string value 1',                   -- c3 string
//  10000000000,                        -- c4 bigint
//  'char value 1',                     -- c5 char(40)
//  1.5,                                -- c6 float
//  123.456,                            -- c7 double
//  0.12345678,                         -- c8 decimal(9,8)
//  1234567890.12345,                   -- c9 decimal(18,5)
//  true,                               -- c10 boolean
//  10,                                 -- c11 smallint
//  timestamp('2023-01-01 12:00:00'),   -- c12 timestamp
//  date('2023-01-01'),                 -- c13 date
//
//  array(1, 2, 3),                     -- c14 array<int>
//  array('a', 'b', 'c'),               -- c15 array<string>
//  named_struct('address', '123 Main St', 'city', 'New York', 'zip', 10001),  -- c16 struct
//  map('key1', 1.1, 'key2', 2.2),       -- c17 map<string,double>
//  array(                              -- c18 array<struct<name:string,value:float>>
//    named_struct('name', 'item1', 'value', 1.1),
//    named_struct('name', 'item2', 'value', 2.2)
//  )
//);
//
//-- 2nd row
//INSERT INTO orc_nested_test VALUES (
//  2,
//  'varchar value 2',
//  'string value 2',
//  20000000000,
//  'char value 2',
//  2.5,
//  234.567,
//  0.23456789,
//  2345678901.23456,
//  false,
//  20,
//  timestamp('2023-02-02 13:30:00'),
//  date('2023-02-02'),
//  array(4, 5, 6),
//  array('d', 'e', 'f'),
//  named_struct('address', '456 Oak Ave', 'city', 'Boston', 'zip', 02108),
//  map('key3', 3.3, 'key4', 4.4),
//  array(
//    named_struct('name', 'item3', 'value', 3.3),
//    named_struct('name', 'item4', 'value', 4.4)
//  )
//);
//
//-- 3rd row
//INSERT INTO orc_nested_test VALUES (
//  3,
//  'varchar value 3',
//  'string value 3',
//  30000000000,
//  'char value 3',
//  3.5,
//  345.678,
//  0.34567890,
//  3456789012.34567,
//  true,
//  30,
//  timestamp('2023-03-03 14:45:00'),
//  date('2023-03-03'),
//  array(7, 8, 9),
//  array('g', 'h', 'i'),
//  named_struct('address', '789 Pine Blvd', 'city', 'Chicago', 'zip', 60601),
//  map('key5', 5.5, 'key6', 6.6),
//  array(
//    named_struct('name', 'item5', 'value', 5.5),
//    named_struct('name', 'item6', 'value', 6.6)
//  )
//);





