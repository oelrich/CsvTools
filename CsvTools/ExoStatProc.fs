module ExoStatProc

open System
open System.IO
open FSharp.Data
open FSharp.Data.CsvExtensions

type RowType = Neg | IPC  | Data

type Row = { RowType: RowType; Name : string; Values: (string * Option<double>) list}

let load_csv (fname:string) (control_name:string) (negatives:string list) (ipcs:string list) =
    let data = CsvFile.Load(fname)
    [for row in data.Rows do
        let control =
            let cell = row.[control_name]
            if String.Equals(cell, "999") then
                None
            else
                Some(row.[control_name].AsFloat(Globalization.CultureInfo.GetCultureInfo("sv-SE")))
        let values =
            [for col in data.Headers.Value.[1..] do
                if String.Equals(row.[col], "999") then
                    yield (col, None)
                else
                    match control with
                    |None -> yield (col, Some(row.[col].AsFloat(Globalization.CultureInfo.GetCultureInfo("sv-SE"))))
                    |Some(ctrl) -> yield (col, Some(row.[col].AsFloat(Globalization.CultureInfo.GetCultureInfo("sv-SE")) - ctrl))]

        let row_type =
            if List.exists (fun name -> String.Equals(name, row.[0].Trim())) negatives then
                Neg
            else if List.exists (fun name -> String.Equals(name, row.[0].Trim())) ipcs then
                IPC
            else
                Data
        let name = row.[0]
        yield {RowType = row_type; Name = name.Replace(",", "").Trim(); Values = values}]

let rec subtract_values (rhs: (string * Option<double>) list) (lhs: (string * Option<double>) list) =
    match rhs,lhs with
    |[],[] -> []
    |(rhs_name,Some(rhs_value))::rest_rhs, (lhs_name,Some(lhs_value))::rest_lhs ->
        if rhs_name = lhs_name then
            (rhs_name, Some(rhs_value - lhs_value)) :: subtract_values rest_rhs rest_lhs
        else
            failwith "That was an odd list ..."
    |(rhs_name,None)::rest_rhs, (lhs_name,_)::rest_lhs ->
        if rhs_name = lhs_name then
            (rhs_name, None) :: subtract_values rest_rhs rest_lhs
        else
            failwith "That was an odd list ..."
    |(rhs_name,_)::rest_rhs, (lhs_name,None)::rest_lhs ->
        if rhs_name = lhs_name then
            (rhs_name, None) :: subtract_values rest_rhs rest_lhs
        else
            failwith "That was an odd list ..."
    |_,_ -> failwith "Eh ..."

let subtract_by_column (rhs:Row) (lhs:Row) =
    let values = subtract_values rhs.Values lhs.Values
    {RowType = rhs.RowType; Name = rhs.Name; Values = values }

let subtract_by_column_rev (rhs:Row) (lhs:Row) =
    let values = subtract_values lhs.Values rhs.Values
    {RowType = rhs.RowType; Name = rhs.Name; Values = values }
        
let rec reduce_by_row (rows:Row list) (sub:Row) =
    match rows with
    |[] -> []
    |row::rest ->
        (subtract_by_column row sub) :: reduce_by_row rest sub

let rec reduce_by_row_rev (rows:Row list) (sub:Row) =
    match rows with
    |[] -> []
    |row::rest ->
        (subtract_by_column_rev row sub) :: reduce_by_row_rev rest sub

let get_median (one:double) (two:double) (three:double) =
    let sorted = [one;two;three] |> List.sort
    sorted.[1]

let rec median_values_rec (rhs: (string * Option<double>) list) (mhs: (string * Option<double>) list) (lhs: (string * Option<double>) list) =
    match rhs,mhs,lhs with
    |[],[],[] -> []
    |(rhs_name,Some(rhs_value))::rest_rhs,(mhs_name,Some(mhs_value))::rest_mhs, (lhs_name,Some(lhs_value))::rest_lhs ->
        if (rhs_name = mhs_name) && (mhs_name = lhs_name) then
            (rhs_name, Some(([rhs_value; mhs_value; lhs_value] |> List.sort).[1])) :: median_values_rec rest_rhs rest_mhs rest_lhs
        else
            failwith "That was an odd list ..."
    |_,_,_ -> failwith "Eh ..."

let median_values (rows:Row list) =
    match rows with
    |rh::mh::lh::[] -> median_values_rec rh.Values mh.Values lh.Values
    |[] -> []
    |_ -> failwith "wrong number of elements in list"

let rec mean_values_rec (rhs: (string * Option<double>) list) (mhs: (string * Option<double>) list) (lhs: (string * Option<double>) list) =
    match rhs,mhs,lhs with
    |[],[],[] -> []
    |(rhs_name,Some(rhs_value))::rest_rhs,(mhs_name,Some(mhs_value))::rest_mhs, (lhs_name,Some(lhs_value))::rest_lhs ->
        (rhs_name, Some((rhs_value + mhs_value + lhs_value) / 3.0)) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |(rhs_name,None)::rest_rhs,_::rest_mhs,_::rest_lhs ->
        (rhs_name, None) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |(rhs_name,_)::rest_rhs,(_,None)::rest_mhs,_::rest_lhs ->
        (rhs_name, None) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |(rhs_name,_)::rest_rhs,_::rest_mhs,(_,None)::rest_lhs ->
        (rhs_name, None) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |_,_,_ -> failwith "Eh ..."

let mean_values (rows:Row list) =
    match rows with
    |rh::mh::lh::[] -> mean_values_rec rh.Values mh.Values lh.Values
    |[] -> []
    |_ -> failwith "wrong number of elements in list"

let rec standard_deviations_rec (rhs: (string * Option<double>) list) (mhs: (string * Option<double>) list) (lhs: (string * Option<double>) list) =
    match rhs,mhs,lhs with
    |[],[],[] -> []
    |(rhs_name,Some(rhs_value))::rest_rhs,(mhs_name,Some(mhs_value))::rest_mhs, (lhs_name,Some(lhs_value))::rest_lhs ->
        let sum = rhs_value + mhs_value + lhs_value
        let mean = sum/3.0
        let deviance =
            Math.Sqrt(Math.Pow(rhs_value - mean, 2.0)/2.0 + Math.Pow(mhs_value - mean, 2.0)/2.0 + Math.Pow(lhs_value - mean, 2.0)/2.0)
        (rhs_name, Some(deviance)) :: standard_deviations_rec rest_rhs rest_mhs rest_lhs
    |(rhs_name,None)::rest_rhs,_::rest_mhs,_::rest_lhs ->
        (rhs_name, None) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |(rhs_name,_)::rest_rhs,(_,None)::rest_mhs,_::rest_lhs ->
        (rhs_name, None) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |(rhs_name,_)::rest_rhs,_::rest_mhs,(_,None)::rest_lhs ->
        (rhs_name, None) :: mean_values_rec rest_rhs rest_mhs rest_lhs
    |_,_,_ -> failwith "Eh ..."

let standard_deviations (rows:Row list) =
    match rows with
    |rh::mh::lh::[] -> standard_deviations_rec rh.Values mh.Values lh.Values
    |[] -> []
    |_ -> failwith "wrong number of elements in list"

let rec print_row_rec (sw:StreamWriter) (data: (string * Option<double>) list) =
    match data with
    |(_,Some(value))::rest ->
        sw.Write("\t")
        sw.Write(value)
        print_row_rec sw rest
    |(_,None)::rest ->
        sw.Write("\tNA")
        print_row_rec sw rest
    |[] -> ()

let print_row (sw:StreamWriter) (row:Row) =
    sw.Write(row.Name)
    print_row_rec sw row.Values
    sw.WriteLine()

let rec print_data_rec (sw:StreamWriter) (rows:Row list) =
    match rows with
    |[] -> ()
    |row::rest ->
        print_row sw row
        print_data_rec sw rest

let rec print_header_rec (sw:StreamWriter) (columns:string list) =
    match columns with
    |[] -> ()
    |col::rest ->
        sw.Write(sprintf "\t%s" col)
        print_header_rec sw rest

let print_header (sw:StreamWriter) (columns:string list) =
    sw.Write ("ID")
    print_header_rec sw columns
    sw.WriteLine()

let print_data (outfile:string) (rows:Row list) =
    let sw = new StreamWriter(outfile)
    print_header sw (rows.Head.Values |> List.map (fun (a,_) -> a))
    print_data_rec sw rows
    sw.Flush()
    sw.Close()
    sw.Dispose()



let gobble_gobble data_file_name processed_file_name external_control negative_controls ipcs =
    let ev_data =
        load_csv data_file_name external_control negative_controls ipcs
        |> List.sortBy (fun elem -> elem.Name)
    
    let ipc_data = List.filter (fun (r:Row) -> r.RowType = IPC) ev_data
    let ipc_median =  {RowType = IPC; Name = "IPC-Median"; Values = median_values ipc_data}

    let ev_data_minus_ipc_median =
        if ipc_median.Values.IsEmpty then
            ev_data
        else
            reduce_by_row ev_data ipc_median
    
    let ev_data_minus_neg_ctrl_mean = reduce_by_row_rev ev_data_minus_ipc_median {RowType = Neg; Name = "Neg-Mean"; Values = mean_values (List.filter (fun (r:Row) -> r.RowType = Neg) ev_data_minus_ipc_median)}

    let neg_ctrl_data = List.filter (fun (r:Row) -> r.RowType = Neg) ev_data_minus_neg_ctrl_mean
    
    let neg_ctrl_mean =  {RowType = Neg; Name = "Neg-Mean"; Values = mean_values neg_ctrl_data}

    let neg_ctrl_sd =  {RowType = Neg; Name = "Neg-SD"; Values = standard_deviations neg_ctrl_data}
    let default_lod =
        {RowType = Neg; Name = "Neg-SD x 3 (LOD)";
         Values = List.map (fun ((n,ov):(string * Option<double>)) ->
            match ov with
            |Some(value) -> (n,Some(value * 3.0))
            |None -> (n,None)) neg_ctrl_sd.Values }


    let data_glob = default_lod :: neg_ctrl_sd :: neg_ctrl_mean :: ipc_median :: ev_data_minus_neg_ctrl_mean
    print_data processed_file_name data_glob



let gobble =
    // Actually works, don't mess with it!
    gobble_gobble  @"D:\Projekt\Lotta\process.csv" @"D:\Projekt\Lotta\processed_data.csv" "119" [ "43_neg-1xRIPA" ; "44_neg-1xRIPA" ; "45_neg-1xRIPA" ] [ "46_IPC" ; "47_IPC"; "48_IPC" ]
    gobble_gobble @"D:\Projekt\Lotta\neuro.csv" @"D:\Projekt\Lotta\neuro_data.csv" "Ext Ctrl" [ "94_neg"; "95_neg"; "96_neg" ] []
    gobble_gobble @"D:\Projekt\Lotta\oncology_v2.csv" @"D:\Projekt\Lotta\oncology_v2_data.csv" "Ext Ctrl" [ "62_neg"; "63_neg"; "64_neg" ] []
    gobble_gobble @"D:\Projekt\Lotta\Inflammation.csv" @"D:\Projekt\Lotta\Inflammation_data.csv" "Ext Ctrl" [ "30_neg"; "31_neg"; "32_neg" ] []
    0