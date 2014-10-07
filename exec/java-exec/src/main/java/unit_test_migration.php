<?php
$myfile = fopen("unit_test_refactoring.txt", "r") or die("Unable to open file!");
$unit_tests = fread($myfile,filesize("unit_test_refactoring.txt"));
$unit_test_lines = explode("\n", $unit_tests);

$file_map = array();

$curr_package = "";
$manual_fix_count = 0;
$auto_fix_count = 0;
$tests = array();
$query_within_single_test_count = 0;
foreach ($unit_test_lines as $line) {
    //echo $line . "\n";
    $line_parts = preg_split( "/\([0-9]/", $line);
    //print_r($line_parts);
    // new package declared, store it
    if ( strpos($line_parts[0], "org.apache") !== FALSE) {
        $curr_package = trim($line_parts[0]);
        echo "PACKAGE:" . $curr_package . "\n";
    } else if ( strpos( $line_parts[0], "()") !== FALSE) {
        // found a test
        $curr_test = str_replace("()", "", trim($line_parts[0]));
        $query_within_single_test_count = 0;
        echo "TEST_METHOD:" . $curr_test . "\n";
    } else if ( strpos( $line_parts[1], " test(") !== FALSE) {
        $test_query = explode(" test(\"", $line_parts[1]);

        $file_path = "../../test/java/" . str_replace(".", "/", $curr_package);
        $file_path .= "/" . $curr_class . ".java";
        $file = fopen($file_path, "rw");
        if ( ! isset( $file_map[ $file_path ] )) {
            $file_map[ $file_path ] = fread($file, filesize($file_path)); 
        }

        // if the 
        if ( (strpos( $test_query[1], "\");") === FALSE && strpos( $test_query[1], "\" );") === FALSE ) || 
            strpos( $test_query[1], "alter session") !== FALSE || strpos( $test_query[1], "alter system") !== FALSE ||
            strpos( $test_query[1], "create view") !== FALSE || strpos( $test_query[1], "drop view") !== FALSE 
            || strpos( $test_query[1], "drop view") !== FALSE ) {
            echo "FIX MANUALLY:" . $curr_test. "." . $test_query[1] . "\n";
            echo "last char:" . $test_query[ 1][strlen($test_query[1]) - 1]. "\n";
            if ($test_query[ 1][strlen($test_query[1]) - 1] != '+'
                && $test_query[ 1][strlen($test_query[1]) - 2] != '+') {
                $manual_fix_count++;
                continue;
            }
            continue;
        }

        $test_info = array();
        $test_info['file path'] = $file_path;
        $test_info['query'] = str_replace("\" );", "", str_replace("\");", "", $test_query[1]));
        $test_info['test method'] = $curr_test;
        $test_info['query count in test'] = $query_within_single_test_count;
        $query_within_single_test_count++;
        $tests[] = $test_info;
        $test_query = explode("\");", $test_query[1]);
        //echo "TEST QUERY:" . $test_query[0] . "\n";
        $auto_fix_count++;
        // TODO - find this in the file and stitch together the entire query
        // nevermind, this is such a rare case I'll just do them manually rather than mess with it
    } else {
        // found a class name
        $curr_class = trim($line_parts[0]);
        echo "=======================\n";
        echo "TEST CLASS:" . $curr_class . "\n";
        echo "=======================\n";
    }
}
echo "MANUAL FIX COUNT:" . $manual_fix_count . "\n";
echo "AUTO FIX COUNT:" . $auto_fix_count . "\n";

$test_queries_to_run = array();
$test_match_count = 0;
foreach ($tests as $test) {
    $file_path = $test['file path'];
    print_r($test['query']);
    echo "\n";
    if ( preg_match( "@" . preg_quote("test(\"" . $test['query']) . "@", $file_map[$file_path])) {
        $test_match_count++;
    }
    $test_queries_to_run[] =
        "testRunAndWriteToFile(UserBitShared.QueryType.SQL,\"" . str_replace(";", "", $test['query']) . "\",\"" . 
            str_replace("../../test/java/org/apache/drill/exec", "exec/java-exec/src/test/resources", str_replace(".java", "", $test["file path"])) . '.' .  $test['test method'] . 
            ($test['query count in test'] != 0 ? '_' . $test['query count in test'] : "") . ".tsv\");";
    // small check to make sure I wasn't messing up any tests that were submitting two queries in a single line
    if (substr_count($test['query'], ";") > 1) {
       throw new Exception("THIS IS BAD");
    }
}
//print_r($test_queries_to_run);
foreach ($test_queries_to_run as $query) {
echo $query . "\n";
}
echo "matched test queries: " . $test_match_count . "\n";
fclose($myfile);

?>
