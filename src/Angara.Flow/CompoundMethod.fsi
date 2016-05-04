module Angara.Execution.Compound

type Input = 
    | NotAvailable
    | Item of Artefact
    | Array of Artefact array 
