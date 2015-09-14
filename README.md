### Introduction
This fork of the <b>[Apache Phoenix](http://phoenix.apache.org/)</b> project modifies the [Upsert Values](https://phoenix.apache.org/language/#upsert_values) grammer to only update/insert the record if a given expression is evaluated to true. Only the row that is being modified can be referenced in the expressions. This allows to check the existing column values before performing the update. The functionality is similar to HBase's [checkAndPut](http://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/HTable.html#checkAndPut%28byte[],%20byte[],%20byte[],%20byte[],%20org.apache.hadoop.hbase.client.Put%29) operation. To support this 'Compare' keyword is added to [upsert values](https://phoenix.apache.org/language/#upsert_values) grammer. If the compare expression evaluates to true, the changes are applied, otherwise the changes are skipped and no errors are raised. Compare expressions support arithmetic and logical operators.

### Examples
```sql
CREATE TABLE User (
	UserId		BIGINT		PRIMARY KEY,
    FirstName	VARCHAR,
    LastName	VARCHAR,
    Phone		VARCHAR,
    Address		VARCHAR,
    PIN			INTEGER
);
    
```

Given that the FirstName is always set for the users, create a user record if one doesn't already exist.
```sql
UPSERT INTO User (UserId, FirstName, LastName, Phone, Address, PIN) VALUES (1, 'Alice', 'A', '123 456 7890', 'Some St. in a city', 1122) COMPARE FirstName IS NULL;
```

Update the phone number for UserId '1' if the FirstName is set. Given that the FirstName is always set for the users, this will only update the record if it already exists.
```sql
UPSERT INTO User (UserId, Phone) VALUES (1, '987 654 3210') COMPARE FirstName IS NOT NULL;
```

Update the phone number if the first name for UserId '1' starts with 'Al' and last name is 'A'
```sql
UPSERT INTO User (UserId, Phone) VALUES (1, '987 654 3210') COMPARE FirstName LIKE 'Al%' AND LastName = 'A';
```

Update the address after verifying the pin
```sql
UPSERT INTO User (UserId, Address) VALUES (1, 'Other St. in the city') COMPARE PIN=1122;
```

The following upsert statements will result in the phone number to be '2222'. Upserts to a single row from a connection are sequentially applied to the row. This ensures that the upserts operate on the latest cell values for a record.
```sql
UPSERT INTO User (UserId, Phone) VALUES (1, '0000');
UPSERT INTO User (UserId, Phone) VALUES (1, '1111') COMPARE Phone = '0000';
UPSERT INTO User (UserId, Phone) VALUES (1, '2222') COMPARE Phone = '1111';
UPSERT INTO User (UserId, Phone) VALUES (1, '3333') COMPARE Phone = '1111';
```

### Limitations

Compare expressions are very similar to where clause expressions. Some of the limitations are
1) Left hand side of the expression should be a column identifier
2) Subqueries are not supported
3) Row key columns can not be used in compare expressions. This is becuase the compare expresions only operates on one row and that row is selected based on the row key value that was passed in the values segment.


Related to:
[PHOENIX-6](https://issues.apache.org/jira/browse/PHOENIX-6)