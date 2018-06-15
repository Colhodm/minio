package csvsql

import (
	"errors"
	"fmt"
	"github.com/xwb1989/sqlparser"
	"math"
	"strconv"
	"strings"
)

//Define errors
var CASTING error = errors.New("Invalid Casting")
var NOTSUPPORTED error = errors.New("Currently not supported")
var FROMNOTDEFINED error = errors.New("From is not defined in your operation")
var UNSUPPORTEDSQLOP error = errors.New("UnsupportedSqlOperation")
var CLASHWITHKEYWORD error = errors.New("400 parse error (no quotation marks: clash with reserved keyword)")
var ASTERIXISNOTALONE error = errors.New("ParseAsteriskIsNotAloneInSelectList")
var CSVINCOMPATIBLE error = errors.New("CSV is not compatible, please reformat")
var AMBIGIOUSCOLS error = errors.New("your column request was ambigious")
var DUPLICATECOLS error = errors.New("you have two columns with the same name")
var INVALIDCOLINDEX error = errors.New("invalid column index")
var INVALIDCOLLABEL error = errors.New("column not located in csv")
var INVALIDOPERAND error = errors.New("operand in SQL expression currently not compatible with API")
var GENERICUNKNOWN error = errors.New("ERROR")

// This is a function which performs the actual aggregation methods on the given row, it uses an array defined the the main parsing function to keep track of values.
func aggregationFunctions(counter int, filteredCounter int, myValues []float64, columnsMap map[string]int, storevalues []string, storeFunctions []string, record []string) error {
	for i := 0; i < len(storeFunctions); i += 1 {
		if storeFunctions[i] == "" {
			i += 1
		} else if storeFunctions[i] == "count" {
			//This if statement is for calculating the count.
			myValues[i] += 1
		} else {
			//case that the columns are in the form of indices instead of column names.
			var convertedFloat float64
			if RepresentsInt(storevalues[i]) {
				convertedFloat, _ = strconv.ParseFloat(storevalues[i], 64)
				//Maybe I should add the error for user info.

			} else {
				//case that the columns are in the form of named columns rather than indices.
				convertedFloat, _ = strconv.ParseFloat(record[columnsMap[trimQuotes(storevalues[i])]], 64)

			}
			//This if statement is for calculating the min.
			if storeFunctions[i] == "min" {
				if counter == -1 {
					myValues[i] = math.MaxFloat64
				}
				if convertedFloat < myValues[i] {
					myValues[i] = convertedFloat
				}

			} else if storeFunctions[i] == "max" {
				//This if statement is for calculating the max
				if counter == -1 {
					myValues[i] = math.SmallestNonzeroFloat64
				}
				if convertedFloat > myValues[i] {
					myValues[i] = convertedFloat
				}

			} else if storeFunctions[i] == "sum" {
				//This if statement is for calculating the sum
				myValues[i] += convertedFloat

			} else if storeFunctions[i] == "avg" {
				//This if statement is for calculating the average
				if filteredCounter == 0 {
					myValues[i] = convertedFloat
				} else {
					//Integer division here is causing a bug where we basically converge to 5 rather than to 10
					myValues[i] = (convertedFloat + (myValues[i] * float64(filteredCounter))) / float64((filteredCounter + 1))
					//fmt.Println(convertedint)
					//fmt.Println(filteredCounter)
				}
			} else {
				return GENERICUNKNOWN

			}

		}
	}
	return nil
}

//This function finds whether a string is in a list
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

//This function returns the index of a string in a list
func stringIndex(a string, list []string) int {
	for i := range list {
		if list[i] == a {
			return i
		}
	}
	return -1
}

// This function parses the SQL expression, and effectively tokenizes it into its seperate parts. It returns the requested column names,alias,limit of records, and the where clause.
func parseSql(sqlInput string) ([]string, string, int, interface{}, []string, error) {
	//return columnNames, alias, limitOfRecords, whereclause, nil

	stmt, err := sqlparser.Parse(sqlInput)
	var whereClause interface{}
	var alias string
	var limit int
	if err != nil {
		fmt.Println(err)
		return nil, "", 0, nil, nil, err

	}
	// Otherwise do something with stmt
	switch stmt := stmt.(type) {
	case *sqlparser.Select:
		//evaluates the where clause
		functionNames := make([]string, len(stmt.SelectExprs))
		columnNames := make([]string, len(stmt.SelectExprs))
		if stmt.Where != nil {

			switch expr := stmt.Where.Expr.(type) {
			default:
				whereClause = expr
			case *sqlparser.ComparisonExpr:
				whereClause = expr
			}
		}
		if stmt.SelectExprs != nil {
			//Need to fix the line below so that it does the type assertion for the entire array

			for i := 0; i < len(stmt.SelectExprs); i += 1 {
				switch expr := stmt.SelectExprs[i].(type) {
				case *sqlparser.StarExpr:
					columnNames[0] = "*"
				case *sqlparser.AliasedExpr:
					switch smallerexpr := expr.Expr.(type) {
					case *sqlparser.FuncExpr:
						if smallerexpr.IsAggregate() {
							functionNames[i] = smallerexpr.Name.CompliantName()
							//Will return function name
							//Case to deal with if we have functions and not an asterix
							switch tempagg := smallerexpr.Exprs[0].(type) {
							case *sqlparser.StarExpr:
								columnNames[0] = "*"
							case *sqlparser.AliasedExpr:
								//fmt.Printf("%#v\n",smallerexpr)
								switch col_name := tempagg.Expr.(type) {
								case *sqlparser.ColName:
									columnNames[i] = col_name.Name.CompliantName()

								}

							}

						} else { //Throw an error useful function for usfmt.
							fmt.Println("errpr")
						}
					case *sqlparser.ColName:
						columnNames[i] = smallerexpr.Name.CompliantName()
					case *sqlparser.SQLVal:
						//Currently an issue related to having spaces in the column name, their parser does not handle this well.
						//fmt.Printf("%#v\n",smallerexpr.Type)

					}
				}
			}

		}
		//This code retrieves the alias and makes sure it is set to the correct value, if not it sets it to the tablename
		if (stmt.From) != nil {
			switch smallerexpr := stmt.From[0].(type) {
			case *sqlparser.AliasedTableExpr:
				alias = smallerexpr.As.CompliantName()
				if alias == "" {
					alias = sqlparser.GetTableName(smallerexpr.Expr).CompliantName()
				}

			}

		}
		if stmt.Limit != nil {
			//Need to fix the line below so that it does the type assertion for the entire array
			switch expr := stmt.Limit.Rowcount.(type) {
			case *sqlparser.SQLVal:
				//The Value of how many rows we're going to limit by
				limit, _ = strconv.Atoi(string(expr.Val[:]))
				//Currently an issue related to having spaces in the column name, their parser does not handle this well.
				//fmt.Printf("%#v\n",smallerexpr.Type)
			}

		}
		return columnNames, alias, limit, whereClause, functionNames, nil
	}
	return nil, "", 0, nil, nil, nil
}

//This is the main function, It goes row by row and for records which validate the where clause it currently prints the appropriate row given the requested columns.
//first return is acutally writer but just testing something
func (reader *Input) processSqlRequest(requestedColumnNames []string, alias string, whereClause interface{}, limitOfRecords int, functionNames []string, myRowVal chan string, myRowState chan string, myRowError chan error) {
	//Adjust this so that we open the appropriate csv file
	//csvFile, _ := os.Open("player.csv")
	//reader := csv.NewReader(bufio.NewReader(csvFile))
	//var b bytes.Buffer
	//writer := bufio.NewWriter(&b)
	//temp, _ := os.Open("player.csv")
	//var inputopts *csvsql.InputOptions = &csvsql.InputOptions{true, ",", "", "player", temp, false}
	//r//eader, err := NewInput(inputCsvOpts)
	//reader :=  inputcsv
	//writer := csv.NewWriter(os.Stdout)
	//Need to add writing capability in instead of just printing stuff
	//err == io.EOF || len(record) == 0
	counter := -1
	filteredCounter := 0
	functionFlag := false
	//My values is used to store our aggregation values if we need to store them.
	myValues := make([]float64, len(requestedColumnNames))
	var columns []string
	//LowercasecolumnsMap is used in accordance with hasDuplicates so that we can raise the error "Ambigious" if a case insensitive column is provided and we have multiple matches.
	lowercaseColumnsMap := make(map[string]int)
	hasDuplicates := make(map[string]bool)
	//ColumnsMap stores our columns and their index.
	columnsMap := make(map[string]int)
	if limitOfRecords == 0 {
		limitOfRecords = 9999999
	}
	for {
		record := reader.ReadRecord()
		if record == nil || len(record) == 0 {

			if functionFlag {
				fmt.Println(myValues)
				myrow := ""
				for i := 0; i < len(myValues); i += 1 {
					aggregateval := strconv.FormatFloat(myValues[i], 'f', 6, 64)
					myrow = myrow + "," + aggregateval
				}
				myRowVal <- myrow
			}
			myRowState <- "Off"
			return
		}
		if counter == -1 {
			columns = reader.Header()
			for i := 0; i < len(columns); i += 1 {
				columns[i] = strings.Replace(columns[i], " ", "_", len(columns[i]))
				if _, exist := columnsMap[columns[i]]; exist {
					myRowError <- DUPLICATECOLS
					return
					//return nil, DUPLICATECOLS
				} else {
					columnsMap[columns[i]] = i
				}
				//This checks that if a key has already been put into the map, that we're setting its appropriate value in has duplicates to be true.
				if _, exist := lowercaseColumnsMap[strings.ToLower(columns[i])]; exist {
					hasDuplicates[strings.ToLower(columns[i])] = true
				} else {
					lowercaseColumnsMap[strings.ToLower(columns[i])] = i
				}
			}
		}
		//When we have reached our limit, on what the user specified as the number of rows they wanted, we terminate our interpreter.
		if limitOfRecords == 0 {
			break
		}
		//The call to the where function clause,ensures that the rows we print match our where clause.
		condition, err := matchesMyWhereClause(record, columnsMap, alias, whereClause)
		if err != nil {
			//fmt.Println(err)
			myRowError <- err
			return
			//return nil, err
		}
		if condition {
			//if its an asterix we just print everything in the row
			if requestedColumnNames[0] == "*" {
				fmt.Println(record)
				myrow := ""
				for i := 0; i < len(record); i += 1 {
					myrow = myrow + "," + record[i]
				}
				myRowVal <- myrow
			} else if alias != "" {

				//COULD BE OPTIMIZED THIS LOOP HAS TO BE RUN EVERY RECORD, uneeded computation
				templist := requestedColumnNames[:]
				for i := 0; i < len(requestedColumnNames); i += 1 {
					//Could have faulty logic here the number shuold be 1 instead of the len of the requestedColumnNames[i]
					//The code below basically cleans the column name of its alias and other syntax, so that we can extract its pure name so that we can utilize our map.
					//might be faulty logic but fixes error the parsers maks in removing the alias
					if !strings.HasPrefix(requestedColumnNames[i], alias) && requestedColumnNames[i][0] == '_' {
						requestedColumnNames[i] = alias + requestedColumnNames[i]
					}

					if strings.Contains(requestedColumnNames[i], ".") {
						templist[i] = strings.Replace(strings.Replace(requestedColumnNames[i], alias+"._", "", len(requestedColumnNames[i])), ",", "", len(requestedColumnNames[i]))
					}
					templist[i] = strings.Replace(strings.Replace(requestedColumnNames[i], alias+"_", "", len(requestedColumnNames[i])), ",", "", len(requestedColumnNames[i]))
					if hasDuplicates[strings.ToLower(templist[i])] {
						//If after cleaning we are able to find a duplicate, it means the clients column was ambigious.
						//log.Fatal("your column request was ambigious")
						myRowError <- AMBIGIOUSCOLS
						return
						//return nil, AMBIGIOUSCOLS
					}
				}
				requestedColumnNames = templist
				//This is for dealing with the case of if we have to deal with a request for a column with an index e.g A_1.
				if RepresentsInt(requestedColumnNames[0]) {
					//This checks whether any aggregation function was called as now we no longer will go through printing each row, and only print at the end
					if len(functionNames) > 0 && functionNames[0] != "" {
						functionFlag = true
						aggregationFunctions(counter, filteredCounter, myValues, columnsMap, requestedColumnNames, functionNames, record)

					} else {
						//The code below finds the appropriate columns of the row given the indicies provided in the SQL request and utilizes the map to retrieve the correct part of the row.
						myrow := ""
						for i := 0; i < len(requestedColumnNames); i += 1 {
							if i == 0 {
								mytempindex, err := strconv.Atoi(requestedColumnNames[0])
								myrow = record[mytempindex]
								if err != nil {
									//fmt.Println("unknown error")
									myRowError <- GENERICUNKNOWN
									return
									//return nil, GENERICUNKNOWN
								}
								if mytempindex > len(columns) {
									//log.Fatal("invalid column index")
									myRowError <- INVALIDCOLINDEX
									return
									//return nil, INVALIDCOLINDEX
								}
							} else {
								mytempindex, err := strconv.Atoi(requestedColumnNames[i])
								myrow = myrow + "," + record[mytempindex]
								if err != nil {
									//fmt.Println("unknown error")
									//return nil, GENERICUNKNOWN
									myRowError <- GENERICUNKNOWN
									return
								}
							}
						}
						fmt.Println(myrow)
						myRowVal <- myrow
					}
				} else {
					//This code does aggregation if we were provided column names in the form of acutal names rather an indices.
					//fmt.Println(requestedColumnNames)
					if len(functionNames) > 0 && functionNames[0] != "" {
						functionFlag = true
						aggregationFunctions(counter, filteredCounter, myValues, columnsMap, requestedColumnNames, functionNames, record)
					} else {
						//This code prints the appropriate part of the row given the filter and select request, if the select request was based on column names rather than indices.
						myrow := ""
						for i := 0; i < len(requestedColumnNames); i += 1 {
							if i == 0 {
								mytempindex, notfound := columnsMap[trimQuotes(requestedColumnNames[0])]
								if !notfound {
									//log.Fatal("column not located in csv")
									myRowError <- INVALIDCOLLABEL
									return
									//return nil, INVALIDCOLLABEL
								}
								myrow = record[mytempindex]
							} else {
								if requestedColumnNames[i] != "" {
									mytempindex, notfound := columnsMap[trimQuotes(requestedColumnNames[i])]
									if !notfound {
										//log.Fatal("column not located in csv")
										//return nil, INVALIDCOLLABEL
										myRowError <- INVALIDCOLLABEL
										return
									}
									myrow = myrow + "," + record[mytempindex]
								}
							}
						}
						fmt.Println(myrow)
						myRowVal <- myrow
					}
				}
			}
			filteredCounter += 1
			limitOfRecords = limitOfRecords - 1
		}
		counter += 1
	}
}

//The function below processes the where clause into an acutal boolean given a row
func matchesMyWhereClause(row []string, columnNames map[string]int, alias string, whereClause interface{}) (bool, error) {
	//if there is no where clause, we just return that the row has the same length as column names, because then this is at least a valid row with no missing entries.
	//return true,nil

	//we check that the lengths are the same, because if there is a row without all of its entires filled, we will get an index out of bounds error, so we are trusting the user to give us a good csv.

	//This particular logic deals with the details of casting, e.g if we have to cast a column of string numbers into int's for comparison.
	var conversionColumn string
	var operator string
	var operand interface{}
	switch expr := whereClause.(type) {
	case *sqlparser.ComparisonExpr:
		operator = expr.Operator
		switch right := expr.Right.(type) {
		case *sqlparser.SQLVal:
			operand = string(right.Val)
			switch right.Type {
			//Returns my type and I can get the appropriate value
			case 0:
				//fmt.Println("String")
			case 1:
				stringversion, isString := operand.(string)
				if isString {
					operand, _ = strconv.Atoi(stringversion)
				}
			}
		}
		switch left := expr.Left.(type) {
		case *sqlparser.SQLVal:
			fmt.Println("Inverted order of stuff")
		case *sqlparser.ColName:
			//fmt.Println()
			//This will return the name of our column
			//fmt.Printf("%#v\n", left.Name.CompliantName())
			conversionColumn = left.Name.CompliantName()
			//fmt.Println()
		}
		//fmt.Printf("%#v\n",expr)
		return evaluateOperator(row[columnNames[conversionColumn]], operator, operand)
	case *sqlparser.AndExpr:
		var leftval bool
		var rightval bool
		switch left := expr.Left.(type) {
		case *sqlparser.SQLVal:
			fmt.Println("Inverted order of stuff")
		case *sqlparser.ColName:
			conversionColumn = left.Name.CompliantName()
		case *sqlparser.ComparisonExpr:
			leftval, _ = matchesMyWhereClause(row, columnNames, alias, left)

		}
		switch right := expr.Right.(type) {
		case *sqlparser.SQLVal:
			fmt.Println("Inverted order of stuff")
		case *sqlparser.ColName:
			conversionColumn = right.Name.CompliantName()
		case *sqlparser.ComparisonExpr:
			rightval, _ = matchesMyWhereClause(row, columnNames, alias, right)

		}

		return (rightval && leftval), nil
	case *sqlparser.OrExpr:
		var leftval bool
		var rightval bool
		switch left := expr.Left.(type) {
		case *sqlparser.SQLVal:
			fmt.Println("Inverted order of stuff")
		case *sqlparser.ColName:
			conversionColumn = left.Name.CompliantName()
		case *sqlparser.ComparisonExpr:
			leftval, _ = matchesMyWhereClause(row, columnNames, alias, left)

		}
		switch right := expr.Right.(type) {
		case *sqlparser.SQLVal:
			fmt.Println("Inverted order of stuff")
		case *sqlparser.ColName:
			conversionColumn = right.Name.CompliantName()
		case *sqlparser.ComparisonExpr:
			rightval, _ = matchesMyWhereClause(row, columnNames, alias, right)
		}
		return (rightval || leftval), nil

	}
	return true, nil
}

//Returns a true or false, whether a string can be represented as an int.
func RepresentsInt(s string) bool {
	if _, err := strconv.Atoi(s); err == nil {
		return true
	}
	return false
}

//This is a really important function it acutally evaluates the boolean statemenet and therefore acutally returns a bool
func evaluateOperator(mytablevalue interface{}, operator string, operand interface{}) (bool, error) {
	//we use type assertion to figure out whether we have a string or an int
	stringversion, _ := mytablevalue.(string)
	operandstringversion, isString := operand.(string)

	//If its a string we want ot make sure that add quotes so that if we had to evaluate a boolean condition with string e.g == 'james' we have equivalent strings
	//if isString {
	//	stringversion = "'" + stringversion + "'"
	//}
	//Sees if it is compatiable to be able to convert to int
	// All the statements below finally acutually encode the logic of the oeperator into our language golang, so as to ensure we can evaluate the boolean statement.
	intversion, isInt := strconv.Atoi(stringversion)
	operandintversion, operandisInt := operand.(int)
	//Basically need some logic thats like, if the types dont match check for a cast
	if operator == ">" {
		if isString && isInt != nil {
			return stringversion > operandstringversion, nil
		} else if isInt == nil && operandisInt {
			return intversion > operandintversion, nil
		}
		return false, CASTING

	} else if operator == "<" {
		if isString && isInt != nil {
			return stringversion < operandstringversion, nil
		} else if isInt == nil && operandisInt {
			return intversion < operandintversion, nil
		}
		return false, CASTING

	} else if operator == "=" {
		if isString && isInt != nil {
			return stringversion == operandstringversion, nil
		} else if isInt == nil && operandisInt {
			return intversion == operandintversion, nil
		}
		return false, CASTING

	} else if operator == "<=" {
		if isString && isInt != nil {
			return stringversion <= operandstringversion, nil
		} else if isInt == nil && operandisInt {
			return intversion <= operandintversion, nil
		}
		return false, CASTING

	} else if operator == ">=" {
		if isString && isInt != nil {
			return stringversion >= operandstringversion, nil
		} else if isInt == nil && operandisInt {
			return intversion >= operandintversion, nil
		}
		return false, CASTING

	} else if operator == "!=" {
		if isString && isInt != nil {
			return stringversion != operandstringversion, nil
		} else if isInt == nil && operandisInt {
			return intversion != operandintversion, nil
		}
		return false, CASTING

	} else {
		//log.Fatal("operand in SQL expression currently not compatible with API")
		return false, INVALIDOPERAND
	}
	return false, nil
}

//this statement just makes it easy to run the SQL parser.
func (csvReader *Input)RunSqlParser(sqlexpression string, myRowVal chan string, myRowState chan string, myRowError chan error) {
	//This creates our input options
	//instead of nil we need to pass in a reader object of our data
	//TODO Add in passing in the channel to my parser so that if I get an error I can make sure it gets written
	random, randomstuff, randomstuffpart, randomvar, functionNames, err := parseSql(sqlexpression)
	if err != nil {
		myRowState <- "ERROR"
		myRowVal <- "ERROR"
		myRowError <- err
	}
	csvReader.processSqlRequest(random, randomstuff, randomvar, randomstuffpart, functionNames, myRowVal, myRowState, myRowError)
}

// func main() {
// 	//some tests i've wrriten, I need to add a lot more to it as well as something to automatically generate a csv.
// 	//Adding Functional Tests
// 	//Test 1 Column Numbers from Alias with a boolean condition
// 	// testing_string := "SELECT A_1, A_2 FROM S3Object as A WHERE last_name = 'James' AND first_name = 'Lebron' LIMIT 5"
// 	///run_sql_parser(testing_string)
// 	//fmt.Println("TEST 1 COMPLETED")

// 	// //Test 2 Most Basic SQL Query
// 	// //Header prints twice figure out why
// 	// testing_string = "SELECT * FROM S3Object"
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 2 COMPLETED")
// 	// //Test 3 Most Basic SQL Query with boolean and No Limit
// 	// //CURRENTLY WE HAVE ERROR
// 	// testing_string = "SELECT * FROM S3Object WHERE last_name == James"
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 3 COMPLETED")

// 	// //Test 4 Most Basic SQL Query with boolean and Limit
// 	// testing_string = "SELECT * FROM S3Object WHERE last_name == James LIMIT 15 "
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 4 COMPLETED")

// 	// //Test 5 Most Basic SQL Query with boolean and Limit and integer casting
// 	// testing_string = "SELECT * FROM S3Object WHERE cast(draft_year as int) > 2012 LIMIT 15 "
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 5 COMPLETED")

// 	// //Test 6 SQL Query with boolean and Limit and integer casting and alias with column numbers
// 	// testing_string = "SELECT A_1, A_2 FROM S3Object as A WHERE cast(draft_year as int) > 2012 LIMIT 15 "
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 6 COMPLETED")

// 	// //Test 7 SQL Query with boolean and Limit and integer casting and alias with column names
// 	// testing_string = "SELECT A_draft_year, A_first_name FROM S3Object as A WHERE cast(draft_year as int) > 2012 LIMIT 15 "
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 7 COMPLETED")

// 	// //Test 8 SQL Query with boolean and  no Limit and integer casting and alias without "as" and with column names
// 	// testing_string = "SELECT A_draft_year, A_first_name FROM S3Object as A WHERE cast(draft_year as int) > 2012"
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 8 COMPLETED")

// 	//Test 9 Tests query with a multiple columns and a single space in between column name
// 	// testing_string := "SELECT S3Object_first_name,z FROM S3Object WHERE cast(draft_year as int) == 2012 LIMIT 10"
// 	// run_sql_parser(testing_string)
// 	// fmt.Println("TEST 9 COMPLETED")

// 	//Test 10 Tests query with a case sensitive column that is invalid
// 	//testing_string := "SELECT A._" + strconv.Quote("random")  +" FROM S3Object as A"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 10 COMPLETED")

// 	//Test 11 Tests query with a case sensitive column that would give an ambigious error without quotes
// 	//testing_string := "SELECT A._" + strconv.Quote("weight")  +" FROM S3Object as A"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 11 COMPLETED")

// 	//Test 12 Tests query with a case sensitive column that would give an ambigious error without quotes
// 	//testing_string := "SELECT A._" + strconv.Quote("WEIGHT")  +" FROM S3Object as A"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 12 COMPLETED")

// 	//Test 13 Tests query with a case sensitive column that would give an ambigious error without quotes, should return an ambigious error
// 	//testing_string := "SELECT A._weight FROM S3Object as A"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 13 COMPLETED")

// 	//Test 14 Tests  aggregation query with a case insensitive column that would give an ambigious error without quotes, should return an ambigious error
// 	//testing_string := "SELECT `cast` FROM S3Object as A"
// 	//testing_string := "select * from S3Object where cast(`draft_year` AS `int`) > 12"
// 	testing_string := "select " + "*" + " from S3OBJECT"
// 	run_sql_parser(testing_string)
// 	fmt.Println("TEST 14 COMPLETED")

// 	//Test 15 Tests  aggregation query with a case sensitive column that would give an ambigious error without quotes, should work fine
// 	//testing_string := "SELECT count(A._" + strconv.Quote(weight) + " FROM S3Object as A"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 15 COMPLETED")

// 	//Test 16 Tests  aggregation query with a column that has a space in its name
// 	//"SELECT count(S3Object_'my name') FROM S3Object WHERE cast(draft_year as int) == 2012"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 16 COMPLETED")

// 	//Test 17 Tests query with a column that has multiple spaces in its name
// 	// /SELECT A_', A_B'
// 	//testing_string := "SELECT " + "first_name" + ", " +strconv.Quote("'my name'") +  " FROM S3Object WHERE draft_year == 2012"
// 	///testing_string := "SELECT " + "sum(S3Object_" + strconv.Quote("'my name is arjun'") + ")" + " FROM S3Object WHERE cast(draft_year as int) == 2012"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 17 COMPLETED")

// 	//Test 18 Tests query with a column that has multiple spaces in its name and is case sensitive
// 	//testing_string := "SELECT S3Object_" + strconv.Quote("'My name'") + " FROM S3Object WHERE cast(draft_year as int) == 2012"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 18 COMPLETED")

// 	//Test 19 Tests garbage query
// 	//"SELECT S3Object_ random"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 19COMPLETED")

// 	//Test 20 Tests garbage query
// 	//"SELECT * from S3Object_ WHERE random"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 20COMPLETED")

// 	//Test 21 Tests garbage query
// 	//"* from S3Object_ WHERE random"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 21 COMPLETED")

// 	//Test 22 Tests reserved words (should fail and give error)
// 	//"SELECT A_cast from S3Object as A
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 22 COMPLETED")

// 	//Test 23 Tests reserved words (should work)
// 	//"SELECT A_" + strconv.Quote("cast") + "from S3Object as A"
// 	//run_sql_parser(testing_string)
// 	//fmt.Println("TEST 22 COMPLETED")

// }

//This function returns a list of columns with the function call removed from the string, and a second column of the appropriate aggregation function to be calculated on that corrosponding column with reference to the first array.
// func calculate_functions(myfunctions []string, columnNames map[string]int) ([]string, []string) {
// 	templist := make([]string, len(myfunctions))
// 	tempoperations := make([]string, len(myfunctions))
// 	for i := 0; i < len(myfunctions); i += 1 {
// 		if strings.Contains(myfunctions[i], "count") {
// 			templist[i] = strings.Replace(strings.Replace(myfunctions[i], "count(", "", 1), ")", "", 1)
// 			tempoperations[i] = "count"
// 		} else if strings.Contains(myfunctions[i], "min") {
// 			templist[i] = strings.Replace(strings.Replace(myfunctions[i], "min(", "", 1), ")", "", 1)
// 			tempoperations[i] = "min"
// 		} else if strings.Contains(myfunctions[i], "max") {
// 			templist[i] = strings.Replace(strings.Replace(myfunctions[i], "max(", "", 1), ")", "", 1)
// 			tempoperations[i] = "max"
// 		} else if strings.Contains(myfunctions[i], "average") {
// 			templist[i] = strings.Replace(strings.Replace(myfunctions[i], "average(", "", 1), ")", "", 1)
// 			tempoperations[i] = "average"
// 		} else if strings.Contains(myfunctions[i], "sum") {
// 			templist[i] = strings.Replace(strings.Replace(myfunctions[i], "sum(", "", 1), ")", "", 1)
// 			tempoperations[i] = "sum"
// 		} else {
// 			//manually encoding this (not good logic) This is to catch the case if we have count("my  name"), however this would not scale to columns with multiple spaces in the column name.
// 			// This issue of spaces between column names is signficant and right now only single spaces are supported This needs to be addressed.
// 			if len(templist) > 0 && templist[1] == "" && tempoperations[0] != "" {
// 				//case for the column with the space in it
// 				return templist, tempoperations
// 			}
// 			return []string{}, []string{}
// 		}
// 	}
// 	return templist, tempoperations
// }

//This method iterates through the requested column names and checks them for potential errors.
// func process_columns_with_spaces(columnNames []string, copy_names []string, position int) int {
// 	//strings.Replace(columnNames[i], "'", "", 1) + " " + strings.Replace(columnNames[i+1], "'", "", 1)
// 	//imagine the case of "my name"
// 	counter := 1
// 	mycolumn := strings.Replace(columnNames[position], "'", "", 1)
// 	for !strings.Contains(columnNames[position+counter], "'") {
// 		mycolumn = mycolumn + " " + columnNames[position+counter]
// 		columnNames[position+counter] = ""
// 		counter += 1
// 	}
// 	mycolumn = mycolumn + " " + strings.Replace(columnNames[position+counter], "'", "", 1)
// 	copy_names[position] = mycolumn
// 	return counter
// }

// func process_columnNames(columnNames []string, alias string) ([]string, error) {
// 	reservedkeywords := []string{"CAST", "SELECT", "FROM", "WHERE", "LIMIT", "count", "sum", "min", "max", "average"}
// 	templist := make([]string, len(columnNames))
// 	for i := 0; i < len(columnNames); i += 1 {
// 		//logic is still faulty we need to go over this
// 		//This statement below checks for the usage of reserved words as columns without being wrapped with the appropriate headers.
// 		if stringInSlice(columnNames[i], reservedkeywords) {
// 			//log.Fatal("400 parse error (no quotation marks: clash with reserved keyword)")
// 			//This is the error case in case we encounter a column requested for which has the same spelling as a reserved word
// 			return []string{}, CLASHWITHKEYWORD
// 		}
// 		//This statement ensures that the user has a valid sql query if they chose to use an asterix.
// 		if len(columnNames) > 1 && columnNames[i] == "*" {
// 			//This is the error case if an asterix is not the only column requested
// 			return []string{}, ASTERIXISNOTALONE
// 		}
// 		//This statement COULD have faulty logic, there is a corner case for which it wouldnt work. This statement is meant to deal with the
// 		//case that there is a space in the middle of a column name. It will work for all cases except when two columns are requested consecutively, and both contain a single quote
// 		templateconditionswithoutalias := strings.Contains(columnNames[i], "'") || strings.Contains(columnNames[i], ("'")) || strings.Contains(columnNames[i], strconv.Quote("'")[:len(strconv.Quote("'"))-1]) || strings.Contains(columnNames[i], strconv.Quote("'")[:len(strconv.Quote("'"))-1])
// 		if strings.Contains(columnNames[i], alias+"_"+"'") || strings.Contains(columnNames[i], (alias+"._"+"'")) || strings.Contains(columnNames[i], alias+"_"+strconv.Quote("'")[:len(strconv.Quote("'"))-1]) || strings.Contains(columnNames[i], alias+"._"+strconv.Quote("'")[:len(strconv.Quote("'"))-1]) || templateconditionswithoutalias {
// 			i += process_columns_with_spaces(columnNames, templist, i)
// 		} else {
// 			templist[i] = columnNames[i]
// 		}

// 	}
// 	return templist, nil
// }

//This function is for evaluating select statements which are case sensitive select "name", we need to trim the quotes to reference our map of columnNames.
func trimQuotes(s string) string {
	if len(s) >= 2 {
		if c := s[len(s)-1]; s[0] == c && (c == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
