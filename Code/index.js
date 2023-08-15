// Single field index 
db.Person.createIndex({age: 1})
// Unique index in order to avoid duplicated document beingg inserted in the future
db.Relationship.createIndex({ relationship: 1, marital_status: 1 }, { unique: true });
db.Occupation.createIndex({ occupation: 1, workclass: 1, hours_per_week:-1 }, { unique: true });
db.Education.createIndex({ education: 1, education_num: -1 }, { unique: true });
// Partial Index
db.Person.createIndex(
    { race: 1 },
    { partialFilterExpression: { native_country: "United-States"}}
);

