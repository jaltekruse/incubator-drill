<?php
$myfile = fopen("unit_test_refactoring.txt", "r") or die("Unable to open file!");
$unit_tests = fread($myfile,filesize("unit_test_refactoring.txt"));
$unit_test_lines = explode("\n", $unit_tests);

$file_map = array();

$curr_package = "";
$manual_fix_count = 0;
$auto_fix_count = 0;
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
        $curr_test = trim($line_parts[0]);
        echo "TEST_METHOD:" . $curr_test . "\n";
    } else if ( strpos( $line_parts[1], " test(") !== FALSE) {
        $test_query = explode(" test(", $line_parts[1]);
        if ( strpos( $test_query[1], "\");") === FALSE) {
            echo "FIX MANUALLY:" . $test_method . "." . $test_query[1] . "\n";
            $manual_fix_count++;

            // too many tests to deal with manually, it just doen't make sense, 
            // and there is too great a chance for errors to be introduced
            $file_path = "../../test/java/" . str_replace(".", "/", $curr_package);
            $file_path .= "/" . $curr_class . ".java";
            $file = fopen($file_path, "rw");
            if ( ! isset( $file_map[ $file_path ] )) {
                $file_map[ $file_path ] = fread($file, filesize($file_path)); 
            }
            continue;
        }
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
print_r($file_map);
fclose($myfile);

?>
