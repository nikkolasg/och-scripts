# simply a wrapper to include all of the decoders tools
Dir[File::dirname(__FILE__) + "/decoders/*.rb"].each do |f|
    require_relative "decoders/#{File::basename(f)}" 
end
