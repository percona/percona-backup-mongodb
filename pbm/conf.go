package pbm

type Conf struct {
	Storage map[string]Storage `bson:"storage"`
}
