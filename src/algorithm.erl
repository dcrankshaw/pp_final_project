%% basic algo:

%% if active:
%% -every node has a parent (originally Null) as well as that parent's connecting edgeweight (originally 0)
%% -find my biggest edge and call the node on the other side N (N is from one timestep before me)
%% -check if my edge weight is bigger than N's current parent edge weight
%% if I am bigger:
%%  tell N -------> N will tell its former parent (if not null) that it needs to get a
%%                  new child
%%  mark N as my child
%% else:
%%  put N in an disqualified list
%%  repeat until I find a valid outgoing node or all my nodes have been disqualified
%% if I have a child I can signal hold or inactive or whatever
%% get reactivated if my child gets claimed by someone else
%% -----------------------------------------------------------

    % VVal = [ParentNode:Weight:ChildNodeA:ChildNodeAWeight:...]
    % VValDefault = [-1,-1:-1,-1]

    % Message to claim a child: {int Node, String "ParentID:EdgeWeight"}
    % Message to bad parent: {int Node, String "reject"}


%%let the vertex name be the global ID and let the VertexValStr be the id of the parent


-module(algorithm).

-export([max_paths/2, debug/0]).
%%-export([max_paths/3]).

%%Tested
process_valstr(ValStr) ->
    TupList = re:split(ValStr, ":"),
    FinalList = lists:map(fun(StrTup) ->
                            OneTuple = re:split(StrTup, ","),
                            [A,B|_] = OneTuple, {A,B} end, TupList),
    FinalList.

%%Tested
get_biggest_edge(ValidEdges) ->
    ReturnList = lists:foldl(fun({NewC, NewW}, {OldC, OldW}) ->
                                Result = max(list_to_integer(NewW),
                                    list_to_integer(OldW)),
                                case Result =:= list_to_integer(NewW) of
                                    true -> {NewC, NewW};
                                    _ -> {OldC, OldW}
                                end end, {"-1","-1"}, ValidEdges),
    ReturnList.

%%Tested
find_new_child(EList, ChildList, VertexValStr) ->

 %%don't look at edges I have already attempted to claim
    ValidEdges = lists:filter(fun({PotentialChild, _}) ->
                            Result = lists:any(fun({InvalChild,_}) -> PotentialChild =:= InvalChild end, ChildList),
                            case Result of
                                true -> false;
                                _ -> true
                            end
                        end, EList),
    %%get biggest valid edge                    
    {NewChild, NewChildWeight} = get_biggest_edge(ValidEdges),
    %%append new child attempt to end of ValString, send message to that child
    Result = case list_to_integer(NewChild) =:= -1 of
                false ->
                    {VertexValStr ++ ":" ++ NewChild ++ "," ++ NewChildWeight,
                    [{list_to_integer(NewChild), NewChild ++ ":"
                        ++ NewChildWeight} | []]};
                _ ->
                    {VertexValStr, []}
            end,
    Result.
            



max_paths({VertexName, VertexValStr, EList}, InMsgs) ->
    io:format("~n[~p]Recvd msgs : ~p ~n",
        [VertexName, InMsgs]),

    GlobalID = list_to_integer(VertexName),
    Snap = GlobalID bsr 32,
    ValList = process_valstr(VertexValStr),
    [ParentInfo|ChildList] = ValList,
    {VInfo, O, S} = 
    case Snap of
        63 ->
            {NewVertexVal, OutMsgs} = find_new_child(EList, ChildList, VertexValStr),
            NewVInfo = {VertexName, NewVertexVal, EList},
            {NewVInfo, OutMsgs, hold};
        
        _ ->
            %%Do I need to find a new Child?
            FindNewChildBool = lists:member("reject", InMsgs),
            {NewVertexVal, OutMsgsChild} = 
            case FindNewChildBool of
                true ->
                    find_new_child(EList, ChildList, VertexValStr);
                _ ->
                    {false, []}
            end,
            
            SplitMessages = lists:filter(fun(CurMessage) ->
                                {_, StrTuple} = re:split(CurMessage, ":"),
                                length(StrTuple) =:= 2 end, InMsgs),                 
            PossibleParents = [ParentInfo|lists:map(fun(CurMessage) ->
                                    {_, StrTuple} = re:split(CurMessage, ":"),
                                    [A,B|_] = StrTuple,
                                    {A,B} end, SplitMessages)],
            NewParent = lists:foldl(fun(CurPos, Max) ->
                                    {_, CurWeight} = CurPos,
                                    {_, MaxWeight} = Max,
                                    case list_to_integer(CurWeight) > list_to_integer(MaxWeight) of
                                        true -> CurPos;
                                        _ -> Max
                                    end
                                end, ParentInfo, PossibleParents),
            RejectedParents = lists:filter(fun(CurMessage) ->
                                        {CurId, _} = CurMessage,
                                        {MaxId, _} = NewParent,
                                        CurId =/= MaxId end, PossibleParents),
            RejectMessages = [{ID, "reject"} || {ID, _} <- RejectedParents],
            OutMsgs = [OutMsgsChild | RejectMessages],
            FinalVertexVal =
            case NewVertexVal of
                false -> VertexValStr;
                _ ->    [_|TempValStr] = process_valstr(NewVertexVal),
                        FinalValStr = [NewParent|TempValStr],
                        FinalTuplesStr = string:join(
                            lists:map(fun({ID,Weight}) -> ID ++ "," ++ Weight end,
                            FinalValStr), ":" ),
                        FinalTuplesStr
            end,
            NewVInfo = {VertexName, FinalVertexVal, EList},
            {NewVInfo, OutMsgs, hold} end,
    
    io:format("[~p]Sending msgs : ~p ~n", [VertexName, O]),
    {VInfo, O, S}.    
                        
            
debug() ->
    .

                        

%%Need to fix NewVertexVal






            



% {_, SplitMessage} = re:split(CurMessage, ":"),
                                        % ThisMessage = lists:any(fun(Message) ->
                                        %         Message =:= "reject" end, SplitMessage),
                                        % ThisMessage end, InMsgs),



