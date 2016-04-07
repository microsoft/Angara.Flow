module Angara.State

open Angara.Graph
open Angara.Data

type VertexState<'vs> = MdMap<int, 'vs>
type VertexIndex = MdKey<int>
type DataFlowState<'v, 'vs when 'v : comparison and 'v :> IVertex> = Map<'v, VertexState<'vs>>
