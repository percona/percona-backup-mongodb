rs.initiate(
    {
        _id: 'rs1',
        members: [
            { _id: 0, host: "rs101:27017" },
            { _id: 1, host: "rs102:27017" },
            { _id: 2, host: "rs103:27017" }
        ]
    }
)