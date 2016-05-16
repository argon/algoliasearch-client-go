package algoliasearch

type Client interface {
	SetExtraHeader(key string, value string)

	SetTimeout(connectTimeout int, readTimeout int)

	ListIndexes() (interface{}, error)
	InitIndex(indexName string) Index
	MoveIndex(source string, destination string) (interface{}, error)
	CopyIndex(source string, destination string) (interface{}, error)

	ListKeys() (interface{}, error)
	AddKey(acl, indexes []string, validity int, maxQueriesPerIPPerHour int, maxHitsPerQuery int) (interface{}, error)
	AddKeyWithParam(params interface{}) (interface{}, error)
	UpdateKey(key string, acl, indexes []string, validity int, maxQueriesPerIPPerHour int, maxHitsPerQuery int) (interface{}, error)
	UpdateKeyWithParam(key string, params interface{}) (interface{}, error)
	GetKey(key string) (interface{}, error)
	DeleteKey(key string) (interface{}, error)

	GetLogs(offset, length int, logType string) (interface{}, error)
	GenerateSecuredApiKey(apiKey string, public interface{}, userToken ...string) (string, error)
	EncodeParams(body interface{}) string

	MultipleQueries(queries []interface{}, optionals ...string) (interface{}, error)

	CustomBatch(queries interface{}) (interface{}, error)
}

type Index interface {
	Delete() (interface{}, error)
	Clear() (interface{}, error)

	GetSettings() (interface{}, error)
	SetSettings(settings interface{}) (interface{}, error)

	GetObject(objectID string, attribute ...string) (interface{}, error)
	GetObjects(objectIDs ...string) (interface{}, error)
	AddObject(object interface{}) (interface{}, error)
	AddObjects(objects interface{}) (interface{}, error)
	UpdateObject(object interface{}) (interface{}, error)
	UpdateObjects(objects interface{}) (interface{}, error)
	PartialUpdateObject(object interface{}) (interface{}, error)
	PartialUpdateObjects(objects interface{}) (interface{}, error)
	DeleteObject(objectID string) (interface{}, error)
	DeleteObjects(objectIDs []string) (interface{}, error)
	DeleteByQuery(query string, params map[string]interface{}) (interface{}, error)

	Batch(objects interface{}, actions []string) (interface{}, error)
	CustomBatch(queries interface{}) (interface{}, error)

	// Deprecated use BrowseFrom or BrowseAll
	Browse(page, hitsPerPage int) (interface{}, error)
	BrowseFrom(params interface{}, cursor string) (interface{}, error)
	BrowseAll(params interface{}) (IndexIterator, error)
	Search(query string, params interface{}) (interface{}, error)

	Copy(name string) (interface{}, error)
	Move(name string) (interface{}, error)

	ListKeys() (interface{}, error)
	GetKey(key string) (interface{}, error)
	AddKey(acl []string, validity int, maxQueriesPerIPPerHour int, maxHitsPerQuery int) (interface{}, error)
	AddKeyWithParam(params interface{}) (interface{}, error)
	UpdateKey(key string, acl []string, validity int, maxQueriesPerIPPerHour int, maxHitsPerQuery int) (interface{}, error)
	UpdateKeyWithParam(key string, params interface{}) (interface{}, error)
	DeleteKey(key string) (interface{}, error)

	WaitTask(task interface{}) (interface{}, error)
	WaitTaskWithInit(taskID float64, timeToWait float64) (interface{}, error)
}

type IndexIterator interface {
	Next() (interface{}, error)
	GetCursor() (string, bool)
}
