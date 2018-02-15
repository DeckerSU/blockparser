<?php

/* 
    (c) Decker, 2018
*/

/*

./parser balances > balances.txt
./parser balances --csv > balances.csv

*/

//define('SATOSHIDEN', "100000000");
define('SATOSHIDEN', "100000000000");
define('MAX_ADDRESSES', 10);
define('MIN_SEND',"0.000001");
//define('COMMAND', "./komodo-cli ");
define('COMMAND', "./komodo-cli -ac_name=PIZZA ");
define('CHECK_VIA_EXPLORER', true);
define('EXPLORER_URL',"http://172.17.112.67:3001/insight-api-komodo");

define('PRE_COMMAND', 'while true; do');
define('POST_COMMAND', '    if [ $? -eq 0 ]; then
        break
    fi
    sleep 1
done
');

//define('DEBUG', true);

$total_value = "0";
$total_addresses = 0;
$total_addresses_diff = 0;
$tx_count = 0;

function file_get_contents_curl($url) {
    $ch = curl_init();

    curl_setopt($ch, CURLOPT_AUTOREFERER, TRUE);
    curl_setopt($ch, CURLOPT_HEADER, 0);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_FOLLOWLOCATION, TRUE);       


    $data["result"] = curl_exec($ch);
    $data["http_code"] = curl_getinfo($ch)["http_code"];
    curl_close($ch);

    return $data;
}

function output_array($arrAList, $check_via_explorer = false) {
    //var_dump($arrAList);
    //sendmany "fromaccount" {"address":amount,...} ( minconf "comment" ["address",...] )
    global $total_value;
    global $total_addresses;
    global $total_addresses_diff;
    global $tx_count;
    
    echo PRE_COMMAND . "\n";
    $i = 0; $error_message = "";
    
    $tx_count++;
    echo "echo -e \"Executing tx \x1B[01;32m#$tx_count\x1B[0m ... \"\n";
    echo "" . COMMAND . 'sendmany "" "{';
    foreach ($arrAList as $address => $satoshi_value) {
        if ($check_via_explorer) {
                         $res = file_get_contents_curl(EXPLORER_URL."/addr/".urlencode($address)."/balance");
                         
                         if (($res["http_code"] == 200))
                            $satoshi_value_explorer = $res["result"]; 
                         else {
                             fwrite(STDERR, "\n\nProblems with Explorer connection ...\n");
                             fwrite(STDERR, var_export($res, true)."\n");
                             exit(1);
                         }
                         
                         if ($satoshi_value==$satoshi_value_explorer) {
                                $value = bcdiv($satoshi_value, SATOSHIDEN, 8);
                                if (bccomp($value, MIN_SEND,8) == -1) $value = MIN_SEND;
                                $total_value = bcadd($total_value, $value, 8);
                                $total_addresses++;
                                if ($i != 0) print(",");
                                echo "\\\"$address\\\":$value";
                         } else {
                                $error_message .= "# Address: $address [V:$satoshi_value;VE:$satoshi_value_explorer]\n";
                                $value = bcdiv($satoshi_value_explorer, SATOSHIDEN, 8);
                                if (bccomp($value, MIN_SEND,8) == -1) $value = MIN_SEND;
                                $total_value = bcadd($total_value, $value, 8);
                                $total_addresses++;
                                $total_addresses_diff++;
                                if ($i != 0) print(",");
                                echo "\\\"$address\\\":$value";
                         }
                    } else
                    { 

                        $value = bcdiv($satoshi_value, SATOSHIDEN, 8);
                        if (bccomp($value, MIN_SEND,8) == -1) $value = MIN_SEND;
                        $total_value = bcadd($total_value, $value, 8);
                        $total_addresses++;

                        if ($i != 0) print(",");
                        echo "\\\"$address\\\":$value";

                        //echo "" . COMMAND . "sendtoaddress \"$address\" $value\n";

                    }
                    //echo "$address;$satoshi_value;$satoshi_value_explorer;".($satoshi_value==$satoshi_value_explorer?"true":"false")."\n";
    $i++;
    }
    echo '}"'."\n";
    echo POST_COMMAND . "\n";
    if ($error_message != "") { 
        //fwrite(STDERR, $error_message);
        echo($error_message);
    }
}


$line_number = 0; 
if (defined('DEBUG')) $count = 0;

if ($file = fopen("balances.csv", "r")) {

    $arrAList = Array();

    while(!feof($file)) {
    $line = fgets($file);
    $line_number++;
	$arr = explode(" ",$line);
	if ($line_number % 1000 == 0) fwrite(STDERR, "Parsing line #$line_number\n");
    
    if (defined('DEBUG'))    
    if ($line_number > 23) { 
        //fwrite(STDERR, "Parsing line #$line_number\n");
        output_array($arrAList, CHECK_VIA_EXPLORER); 
        fclose($file);
        fwrite(STDERR, "Total lines: $line_number\n");
        fwrite(STDERR, "Total addresses: $total_addresses\n");
        fwrite(STDERR, "Total addresses diff: $total_addresses_diff\n");
        fwrite(STDERR, "Total value: $total_value\n");
        exit(1); 
    }

    // parse address only if have positive balance (!), because (!) trim("0") == false
    if (trim($arr[0])) {
            $satoshi_value = trim($arr[0]);
            $address = trim($arr[1]);
            if (defined('DEBUG')) { $count++; echo "# [$count]\"$address\" $satoshi_value\n"; }
            // if address exists in list add satoshi value, if address is new - creates new one array member
            if (count($arrAList) < MAX_ADDRESSES) {
                if (array_key_exists($address,$arrAList)) {
                    $arrAList[$address] = bcadd($arrAList[$address],$satoshi_value,8);
                } else $arrAList[$address] = $satoshi_value;
                
            } else {
                output_array($arrAList, CHECK_VIA_EXPLORER);
                $arrAList = Array();
                if (array_key_exists($address,$arrAList)) {
                    $arrAList[$address] = bcadd($arrAList[$address],$satoshi_value,8);
                } else $arrAList[$address] = $satoshi_value;
            }
	        
	}
    }
    output_array($arrAList, CHECK_VIA_EXPLORER); // last addresses
    fclose($file);
    fwrite(STDERR, "Total lines: $line_number\n");
    fwrite(STDERR, "Total addresses: $total_addresses\n");
    fwrite(STDERR, "Total addresses diff: $total_addresses_diff\n");
    fwrite(STDERR, "Total value: $total_value\n");
}

?>