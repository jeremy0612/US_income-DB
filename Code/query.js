//The number of male who is working more than 30 hours per week
db.Person.aggregate([
    {
        $lookup: 
        {
            from: "Occupation",
            localField: "Occupation_Id",
            foreignField: "_id",
            as: "Occupation"
        }
    },
    {
        $match: 
        {
            gender: "Male",
            "Occupation.hours_per_week": { $gt: 30 }
        }
    },
    {
        $count: "total"
    }
]).toArray()[0].total;
//The number of people living in the United States whose income more than 50K
db.Person.find(
{
    native_country: "United-States",
    "Finance.income_bracket": '>50K'
}).count();
//Total balance of all people living in the United States
db.Person.aggregate([
    {
        $match: {native_country: "United-States"}
    },
    {
        $group: 
        {
            _id: null,
            total: { $sum: "$Finance.total" }
        }
    }
]).toArray()[0].total;
//Total working hours per week of all people whose income is equal or lower than 50K
db.Person.aggregate([
    {
        $match: { "Finance.income_bracket": "<=50K" }
    },
    {
        $lookup: 
        {
            from: "Occupation",
            localField: "Occupation_Id",
            foreignField: "_id",
            as: "Occupation"
        }
    },
    {
        $unwind: "$Occupation"
    },
    {
        $group: 
        {
            _id: null,
            total: { $sum: "$Occupation.hours_per_week" }
        }
    }
]).toArray()[0].total;
//The number of people have total balance more than 100000 and working hours per week lower than 55
db.Person.aggregate([
    {
        $match: { "Finance.total": {$gt: 100000} }
    },
    {
        $lookup: 
        {
            from: "Occupation",
            localField: "Occupation_Id",
            foreignField: "_id",
            as: "Occupation"
        }
    },
    {
        $project:
        {
            hours: {$sum: "$Occupation.hours_per_week"}
        }
    },
    {
        $match: {hours: {$lt: 55}}
    },
    {
        $count: "total"
    }
]).toArray()[0].total;

