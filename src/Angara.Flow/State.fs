module Angara.State

open Angara.Graph
open Angara.Data

type MdVertexState<'vs> = MdMap<int, 'vs>
type VertexIndex = int list
type DataFlowState<'v, 'vs when 'v : comparison and 'v :> IVertex> = Map<'v, MdVertexState<'vs>>
