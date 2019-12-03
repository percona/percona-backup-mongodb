rs.initiate(
    {
        _id: 'rs2',
        members: [
            { _id: 0, host: "rs201:27017" },
            { _id: 1, host: "rs202:27017" },
            { _id: 2, host: "rs202:27017" }
        ]
    }
)