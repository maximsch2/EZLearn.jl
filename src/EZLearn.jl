module EZLearn

using OBOParse
using SQLite
using JSON

abstract ClassifierView
abstract Belief

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
        @show v
        labels = construct_labels(v, task.beliefs, task)
        belief, model = train_and_predict(v, labels, task)
        push!(task.beliefs[v.id], BeliefDict(belief))
        push!(task.models[v.id], model)
    end
end

include("beliefs.jl")

end