__precompile__()

module EZLearn

using OBOParse
using SQLite
using JSON

abstract type ClassifierView end
abstract type Belief end

immutable EZLearnTask
    views::Vector{ClassifierView}
    beliefs::Dict{String, Vector{Belief}}
    models::Dict{String, Vector{Any}}
    ontology::Ontology
    params::Dict
    all_samples::Vector{String}
    cache
end

function ezlearn_step(task::EZLearnTask)
    for v in task.views
        @show typeof(v)
        labels = construct_labels(v, task.beliefs, task)
        belief, model = train_and_predict(v, labels, task)
        push!(task.beliefs[v.id], BeliefDict(belief))
        push!(task.models[v.id], model)
    end
end

train_and_predict(v::ClassifierView, labels, task) = error("not implemented")
construct_labels(v::ClassifierView, beliefs, task) = error("not implemented")

include("beliefs.jl")

export EZLearnTask, BeliefDict, BeliefSQLite, get_beliefs, intersect_labels_core, intersect_labels_simple, ezlearn_step, store_beliefs, get_all_samples

end