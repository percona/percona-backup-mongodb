package log

import "fmt"

func ExampleLevel_Enabled() {
	fmt.Println(ParseLevel("info").Enabled(ParseLevel("error")))
	fmt.Println(ParseLevel("info").Enabled(ParseLevel("warning")))
	fmt.Println(ParseLevel("info").Enabled(ParseLevel("info")))
	fmt.Println(ParseLevel("info").Enabled(ParseLevel("debug")))

	// behavior with unsupported levels
	fmt.Println(ParseLevel("info").Enabled(ParseLevel("")))
	fmt.Println(ParseLevel("").Enabled(ParseLevel("error")))
	fmt.Println(ParseLevel("").Enabled(ParseLevel("")))

	// Output: true
	// true
	// true
	// false
	// true
	// false
	// true
}
