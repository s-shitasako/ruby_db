module RubyDB::Config
  def self.file_root
    File.join File.dirname(__FILE__), 'db'
  end
end
