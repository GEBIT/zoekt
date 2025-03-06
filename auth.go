// initially taken from https://usrbinpehli.medium.com/user-authentication-via-ldap-in-go-aff096654db5

package zoekt

import (
	"fmt"
	"log"
	"os"
	"strings"

	ldap "github.com/go-ldap/ldap/v3"
)

// UserLogin struct represents the user credentials.
type UserLogin struct {
	Username string
	Password string
}

// Connect establishes a connection to the LDAP server as the bind user.
func Connect() (*ldap.Conn, error) {
	conn, err := ldap.DialURL(os.Getenv("LDAP_ADDRESS"))
	if err != nil {
		log.Printf("LDAP connection failed, error details: %v", err)
		return nil, err
	}

	if err := conn.Bind(os.Getenv("BIND_USER"), os.Getenv("BIND_PASSWORD")); err != nil {
		log.Printf("LDAP bind failed while connecting, error details: %v", err)
		return nil, err
	}

	return conn, nil
}

// Auth performs LDAP authentication for the provided user credentials.
func Auth(conn *ldap.Conn, user UserLogin) (bool, []error) {
	// split the base dns by delimiter
	ldapBaseDns := strings.Split(os.Getenv("LDAP_BASE_DNS"), "|")
	// create search responses map for the results
	searchResps := map[*ldap.SearchResult][]error{}
	// loop over each basedn
	for _, baseDn := range ldapBaseDns {
		// create search request
		searchRequest := ldap.NewSearchRequest(
			baseDn,
			ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
			fmt.Sprintf("(sAMAccountName=%s)", user.Username),
			[]string{"dn"},
			nil,
		)
		// do the actual user search
		searchResp, err := conn.Search(searchRequest)
		// initialize result
		searchResps[searchResp] = []error{}
		if err != nil {
			searchResps[searchResp] = append(searchResps[searchResp], err)
		}

		if len(searchResp.Entries) != 1 {
			err = fmt.Errorf("user: %s not found or multiple entries found", user.Username)
			searchResps[searchResp] = append(searchResps[searchResp], err)
		}
	}

	// loop over each search result
	for searchResp, errs := range searchResps {
		if len(errs) == 0 {
			// this search result didn#t yieald an errors, try to bind as this user
			userDN := searchResp.Entries[0].DN

			err := conn.Bind(userDN, user.Password)
			if err != nil {
				err = fmt.Errorf("LDAP authentication failed for user %s", user.Username)
				searchResps[searchResp] = append(searchResps[searchResp], err)
				// continue the loop, checking the next result
				continue
			}
			// the bind was successful
			return true, nil
		}
	}

	// no bind was successful, collect all errors
	totalErrs := []error{}
	for _, errs := range searchResps {
		totalErrs = append(totalErrs, errs...)
	}
	// and return them
	return false, totalErrs
}
