rs.initiate(
    {
        _id: 'cfg',
        configsvr: true,
        version: 1,
        members: [
            { _id: 0, host: "cfg01:27017" },
            { _id: 1, host: "cfg02:27017" },
            { _id: 2, host: "cfg02:27017" }
        ]
    }
)