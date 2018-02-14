<?php

/*

./parser balances > balances.txt
./parser balances --csv > balances.csv

*/

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




$line_number = 0;
if ($file = fopen("balances.csv", "r")) {
    while(!feof($file)) {
        $line = fgets($file);
        $line_number++;
	$arr = explode(" ",$line);
	if ($line_number % 1000 == 0)
	fwrite(STDERR, "Parsing line #$line_number\n");
	// parse address only if have positive balance (!), because (!) trim("0") == false
	if (trim($arr[0])) {
		$satoshi_value = trim($arr[0]);
	        $address = trim($arr[1]);

		$check_via_explorer = false;
		
		if ($check_via_explorer) {
		$res = file_get_contents_curl("http://127.0.0.1:3001/insight-api-komodo/addr/".urlencode($address)."/balance");
                $satoshi_value_explorer = $res["result"];
                if ($satoshi_value==$satoshi_value_explorer) {
	                $value = bcdiv($satoshi_value, "100000000", 8);
			echo "./komodo-cli sendtoaddress \"$address\" $value\n";

		} else {
			$value = bcdiv($satoshi_value_explorer, "100000000", 8);
		        echo "./komodo-cli sendtoaddress \"$address\" $value # $satoshi_value;$satoshi_value_explorer\n";
			//fwrite(STDERR, "$address;$satoshi_value;$satoshi_value_explorer;".($satoshi_value==$satoshi_value_explorer?"true":"false")."\n");
		}
		} else
		{ 
		        $value = bcdiv($satoshi_value, "100000000", 8);
			echo "./komodo-cli sendtoaddress \"$address\" $value\n";
		}

		//echo "$address;$satoshi_value;$satoshi_value_explorer;".($satoshi_value==$satoshi_value_explorer?"true":"false")."\n";
	        
	}
    }
    fclose($file);
}

?>