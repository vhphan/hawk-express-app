SELECT SUM(value)
FROM unnest(ARRAY[1, 2, 4, null]) AS value;

