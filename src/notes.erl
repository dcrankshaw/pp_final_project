

shortest_path({VName, VValStr, EList}, LargestTillNow, InMsgs) ->


  io:format("~n[~p]Recvd msgs : ~p : Aggregate : ~p ~n", 
            [VName, InMsgs, LargestTillNow]),



  LTN = list_to_integer(LargestTillNow),
  NewLTN = 
    lists:foldl(
      fun({_, TV}, OldLTN) ->
          TestLTN = list_to_integer(TV),
          case TestLTN > OldLTN of
            true -> TestLTN;
            _ -> OldLTN
          end
      end, LTN, [{0, VName}|EList]),



  {VInfo, O, NAgg, S} = 
    case VName of
      "1" ->
        OutMsgs = [{TV, VName} || {_, TV} <- EList],
        {{VName, "_", EList}, OutMsgs, integer_to_list(NewLTN), hold};
      _ ->
        case InMsgs of
          [] -> {{VName, "inf", EList}, [], LargestTillNow, hold};
          _ ->
            StartVal = case VValStr of VName -> "inf"; _ -> VValStr end,
            Shortest =
              lists:foldl(
                fun(Msg, CurrSh) ->
                    Split = re:split(Msg, ":", [{return, list}]),
                    case (["inf"] =:= CurrSh) or 
                      (length(Split) < length(CurrSh)) of
                      true -> Split;  %%recursion
                      _ -> CurrSh
                    end
                end, re:split(StartVal, ":", [{return, list}]),
                InMsgs),
            NewVVal = string:join(Shortest, ":"),
            OutMsgs =
              [{TV, VName ++ ":" ++ NewVVal} || {_, TV} <- EList],
            {{VName, NewVVal, EList}, OutMsgs, integer_to_list(NewLTN), hold}
        end
    end,                            
  io:format("[~p]Sending msgs : ~p ~n", [VName, O]),
  {VInfo, O, NAgg, S}.