//----------------------- Documents Format for each Collections --------------------
//collection raw
{
    _id: ObjectId("649268ed277d9bb3259d7ad6"),
    age: 40,
    workclass: 'Private',
    total: 121772,
    education: 'Assoc-voc',
    education_num: 11,
    marital_status: 'Married-civ-spouse',
    occupation: 'Craft-repair',
    relationship: 'Husband',
    race: 'Asian-Pac-Islander',
    gender: 'Male',
    capital_gain: 0,
    capital_loss: 0,
    hours_per_week: 40,
    native_country: '?',
    income_bracket: '>50K'
}
//collection Person
{
    age: 40,
    gender: 'Male',
    Occupation_Id: ObjectId(), // Id of the occupation related to
    Education_Id: ObjectId(), //Id of the Education related to
    Relationship_Id: ObjectId(), // Id of the Relationship related to
    race: 'Asian-Pac-Islander',
    native_country: '?',
    Finance:
    {
        capital_gain: 0,
        capital_loss: 0,
        total: 121772,
        income_bracket: '>50K'
    }
}
//Occupation collection
{
    occupation: 'Craft-repair',
    hours_per_week: 40,
    workclass: 'Private'
}
//Education collection
{
    education: 'Assoc-voc',
    education_num: 11
}
//Relationship collection
{
    relationship: 'Husband',
    marital_status: 'Married-civ-spouse'
}
//------------------------ Detach data from raw collection -----------------------
// Iterate over the documents in the 'raw' collection
db.raw.find().forEach((document) => 
{
    // Detach 'Occupation' document
    const occupationDocument = {
        occupation: document.occupation,
        hours_per_week: document.hours_per_week,
        workclass: document.workclass,
    };
    db.Occupation.insertOne(occupationDocument);
    // Detach 'Education' document
    const educationDocument = {
        education: document.education,
        education_num: document.education_num,
    };
    db.Education.insertOne(educationDocument);
    // Detach 'Relationship' document
    const relationshipDocument = {
        relationship: document.relationship,
        marital_status: document.marital_status,
    };
    db.Relationship.insertOne(relationshipDocument);
});
//Remove all duplicated data from Occupation,Education and Relationship collection

db.Relationship.aggregate(
[
    {
        $group:
        {
            _id:
            {
                _id: ObjectId(),
                relationship: '$relationship',
                marital_status: '$marital_status',
            }
        }   
    },
    {
        $project:
        {
            _id: 0,
            relationship: '$_id.relationship',
            marital_status: '$_id.marital_status',
        }
    },
    {
        $out: "Relationship"
    }
]);
db.Education.aggregate(
[
    {
        $group:
        {
            _id:
            {
                _id: ObjectId(),
                education: '$education',
                education_num: '$education_num',
            }
        }   
    },
    {
        $project:
        {
            _id: 0,
            education: '$_id.education',
            education_num: '$_id.education_num',
        }
    },
    {
        $out: "Education"
    }
]);
db.Occupation.aggregate(
[
    {
        $group:
        {
            _id:
            {
                _id: ObjectId(),
                occupation: '$occupation',
                hours_per_week: '$hours_per_week',
                workclass: '$workclass'
            }
        }   
    },
    {
        $project:
        {
            _id: 0,
            occupation: '$_id.occupation',
            hours_per_week: '$_id.hours_per_week',
            workclass: '$_id.workclass'
        }
    },
    {
        $out: "Occupation"
    }
]);
// Insert data into Person collection
db.raw.find().forEach((document) => 
{
    // Assign ObjectId referenced to Relationship document
    const RelationshipDocument = db.Relationship.findOne({
    $and: [
        { relationship: document.relationship },
        { marital_status: document.marital_status }
    ]
    });
    const RelationshipId = RelationshipDocument ? RelationshipDocument._id : null;
    // Assign ObjectId referenced to Education document
    const EducationDocument = db.Education.findOne({
    $and: [
        { education: document.education },
        { education_num: document.education_num }
    ]
    });
    const EducationId = EducationDocument ? EducationDocument._id : null;
    // Assign ObjectId referenced to Occcupation document
    const OccupationDocument = db.Occupation.findOne({
    $and: [
        { occupation: document.occupation },
        { hours_per_week: document.hours_per_week },
        { workclass: document.workclass }
    ]
    });
    const OccupationId = OccupationDocument ? OccupationDocument._id : null;
    // Detach 'Person' document
    const personDocument = 
    {
        age: document.age,
        gender: document.gender,

        Occupation_Id: OccupationId,
        Education_Id: EducationId,
        Relationship_Id: RelationshipId,

        race: document.race,
        native_country: document.native_country,
        Finance: {
          capital_gain: document.capital_gain,
          capital_loss: document.capital_loss,
          total: document.total,
          income_bracket: document.income_bracket,
        },
    };
    db.Person.insertOne(personDocument);
});
//--------------------------------------------------------------------------------------