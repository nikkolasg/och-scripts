Dir[File::dirname(__FILE__)+"/insert/*.rb"].each do |f|
    require_relative "insert/#{File::basename(f)}"
end
