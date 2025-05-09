package influxdb

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"os"

	"github.com/influxdata/influxdb1-client/models"
	client "github.com/influxdata/influxdb1-client/v2"

	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	assert := assert.New(t)

	conn, err := Connect(map[string]interface{}{
		"address":              "",
		"username":             "",
		"password":             "",
		"insecure_skip_verify": true,
	})
	assert.Nil(conn)
	assert.Error(err)

	conn, err = Connect(map[string]interface{}{
		"address":  "http://localhost",
		"database": "",
		"username": "",
		"password": "",
	})
	assert.Nil(conn)
	assert.Error(err)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	conn, err = Connect(map[string]interface{}{
		"address":  srv.URL,
		"database": "",
		"username": "",
		"password": "",
	})

	assert.NotNil(conn)
	assert.NoError(err)
}

func TestAddPoint(t *testing.T) {
	assert := assert.New(t)

	// Test add Point without tags
	connection := &Connection{
		config: map[string]interface{}{},
		points: make(chan *client.Point, 1),
	}

	connection.addPoint("name", models.Tags{}, models.Fields{"clients.total": 10}, time.Now())
	point := <-connection.points
	assert.NotNil(point)
	tags := point.Tags()
	assert.NotNil(tags)
	assert.NotEqual(tags["testtag2"], "value")

	// Test add Point with tags
	connection.config["tags"] = map[string]interface{}{
		"testtag": "value",
	}

	connection.addPoint("name", models.Tags{}, models.Fields{"clients.total": 10}, time.Now())
	point = <-connection.points
	assert.NotNil(point)
	tags = point.Tags()
	assert.NotNil(tags)
	assert.Equal(tags["testtag"], "value")
	assert.NotEqual(tags["testtag2"], "value")

	// Tried to overright by config
	connection.config["tags"] = map[string]interface{}{
		"nodeid": "value",
	}

	tagsOrigin := models.Tags{}
	tagsOrigin.SetString("nodeid", "collected")

	connection.addPoint("name", tagsOrigin, models.Fields{"clients.total": 10}, time.Now())
	point = <-connection.points
	assert.NotNil(point)
	tags = point.Tags()
	assert.NotNil(tags)
	assert.Equal(tags["nodeid"], "collected")

	// Test panic if it was not possible to create a point
	assert.Panics(func() {
		connection.addPoint("name", models.Tags{}, nil, time.Now())
	})
}

func TestAddPointWithInvalidCharacters(t *testing.T) {
	assert := assert.New(t)

	connection := &Connection{
		config: map[string]interface{}{},
		points: make(chan *client.Point, 1),
	}

	tagsOrigin := models.Tags{}
	tagsOrigin.SetString("owner", "\u00a0this owner\nuses invalid chars\t")

	connection.addPoint("name", tagsOrigin, models.Fields{"clients.total": 10}, time.Now())
	point := <-connection.points
	assert.NotNil(point)
	tags := point.Tags()
	assert.NotNil(tags)
	assert.Equal(tags["owner"], " this owner uses invalid chars ")
}

func TestAddPointWithValidCharacters(t *testing.T) {
	assert := assert.New(t)

	connection := &Connection{
		config: map[string]interface{}{},
		points: make(chan *client.Point, 1),
	}

	tagsOrigin := models.Tags{}
	tagsOrigin.SetString("owner", "📶this owner uses only\u0020valid chars🛜")

	connection.addPoint("name", tagsOrigin, models.Fields{"clients.total": 10}, time.Now())
	point := <-connection.points
	assert.NotNil(point)
	tags := point.Tags()
	assert.NotNil(tags)
	assert.Equal(tags["owner"], "📶this owner uses only\u0020valid chars🛜")
}

func TestPassword(t *testing.T) {
	assert := assert.New(t)

	// Test clear text password
	assert.Equal("testpassword", Config(map[string]interface{}{
		"password": "testpassword",
	}).Password())

	// Test with password not equal
	assert.NotEqual("testpassword", Config(map[string]interface{}{
		"password": "testpa33woru",
	}).Password())

	// Test empty text password
	assert.Equal("", Config(map[string]interface{}{
		"password": "",
	}).Password())

	// Test no password
	assert.Equal("", Config(map[string]interface{}{
		"address":  "http://localhost",
	}).Password())

	// Test password from file with password parameter
	assert.Equal("Extr3MePAssWORDfromFiLE", Config(map[string]interface{}{
		"password": "",
		"password_file": "testdata/password.txt",
	}).Password())

	// Test password from file with value set to password parameter, to test priority of fields
	assert.Equal("Extr3MePAssWORDfromFiLE", Config(map[string]interface{}{
		"password": "NotTheFilePassword",
		"password_file": "testdata/password.txt",
	}).Password())

	// Test password from file without defining password parameter
	assert.Equal("Extr3MePAssWORDfromFiLE", Config(map[string]interface{}{
		"password_file": "testdata/password.txt",
	}).Password())

	// Test not equal password from file without defining password parameter
	assert.NotEqual("SOMETHING_DIFFeReNT", Config(map[string]interface{}{
		"password_file": "testdata/password.txt",
	}).Password())

	// Test password from file with line breaks
	assert.Equal("EXTREMLYDIFFERENTPASSWORD", Config(map[string]interface{}{
		"password_file": "testdata/password-with-whitepaces.txt",
	}).Password())

	// Test not equal password from file with line breaks
	assert.NotEqual("THIS_IS_NOT_THE_SAME", Config(map[string]interface{}{
		"password_file": "testdata/password-with-whitepaces.txt",
	}).Password())

	// Test with environment variable in password_file path
	if err := os.Setenv("CREDENTIALS_DIRECTORY", "testdata"); err != nil {
		t.Fatalf("os.Setenv failed: %v", err)
	}
	defer func() {
		if err := os.Unsetenv("CREDENTIALS_DIRECTORY"); err != nil {
			t.Logf("failed to unset CREDENTIALS_DIRECTORY: %v", err)
		}
	}()
	assert.Equal("Extr3MePAssWORDfromFiLE", Config(map[string]interface{}{
		"password_file": "${CREDENTIALS_DIRECTORY}/password.txt",
	}).Password())
	assert.NotEqual("TEST_FROM_FILE_NOT_EQUAL", Config(map[string]interface{}{
		"password_file": "${CREDENTIALS_DIRECTORY}/password.txt",
	}).Password())

}

func TestUsername(t *testing.T) {
	assert := assert.New(t)

	// Testing username set
	assert.Equal("usernAme", Config(map[string]interface{}{
		"username": "usernAme",
	}).Username())

	// Test empty username
	assert.Equal("", Config(map[string]interface{}{
		"username": "",
	}).Username())

	// Test no username
	assert.Equal("", Config(map[string]interface{}{
		"address":  "http://localhost",
	}).Username())

}
