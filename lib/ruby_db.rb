#
# RubyDB
#
#   Ruby database library which stores data on the local file system.
#
#   https://github.com/s-shitasako/ruby_db
#

class RubyDB
  class Error < StandardError; end

  module TableSettings
    def id_name(name = nil)
      name ? @id_name = name : (@id_name || :id)
    end

    def field(field = nil)
      field ? @field = field : (@field.merge id_name => :integer)
    end

    def unique(*keys)
      @unique = keys
    end

    def index(*keys)
      @index = keys
    end

    def table_name
      name.gsub(/\W/, '')
        .sub(/\A([A-Z])/){$1.downcase}
        .gsub(/([A-Z])/){"_#{$1.downcase}"}
    end
  end

  module MiddlewareControl
    def middleware
      @middleware ||= begin
        middleware = Middleware.new table_name
        middleware.id_name = @id_name || :id
        middleware.field = @field
        middleware.index = (@index || []) + (@unique || [])
        middleware
      end
    end

    def select(query)
      middleware.select query
    end

    def insert(record)
      middleware.insert record
    end

    def update(query, record)
      middleware.update(query, record)
    end

    def delete(query)
      middleware.delete query
    end
  end

  module QueryOperations
    def where(query)
      inherit query
    end

    def find_by(query)
      klass.find_record merge_query query
    end

    def all
      klass.find_records @query
    end
    alias to_a all

    def first
      all.first
    end

    def last
      all.last
    end

    def destroy
      klass.delete @query
    end

    private
    def klass
      @klass || self
    end

    def merge_query(query)
      @query ? @query.merge(query) : query
    end

    def inherit(query)
      Query.new klass, merge_query(query)
    end
  end

  def initialize(attrs = {}, persisted = false)
    @persisted = persisted
    self.class.field.keys.each do |name|
      instance_variable_set :"@#{name}", attrs[name]
    end
  end

  def self.create(attrs)
    record = new attrs
    record.save
    record
  end

  def self.retrieve(attrs)
    attrs && new(attrs, true)
  end

  def self.find_record(query)
    retrieve select(query).first
  end

  def self.find_records(query)
    select(query).map!{|r| retrieve r}
  end

  def save
    if persisted?
      self.class.update to_query, to_record
    else
      instance_variable_set(
        :"@#{self.class.id_name}",
        self.class.insert(to_record)
      )
      @persisted = true
    end
  end

  def destroy
    self.class.delete to_query
  end

  def persisted?
    @persisted
  end

  private
  def method_missing(name, *args)
    if self.class.field.has_key? name
      instance_variable_get :"@#{name}"
    elsif name.to_s =~ /\=\z/
      field_name = name.to_s.sub(/\=\z/, '').to_sym
      if self.class.field.has_key? field_name
        instance_variable_set :"@#{field_name}", *args
      else
        super
      end
    else
      super
    end
  end

  def to_record
    self.class.field.keys.reduce({}) do |record, field|
      variable = instance_variable_get :"@#{field}"
      record[field] = variable unless variable.nil?
      record
    end
  end

  def to_query
    raise unless persisted?
    id_name = self.class.id_name
    {id_name => instance_variable_get(:"@#{id_name}")}
  end

  extend TableSettings
  extend MiddlewareControl
  extend QueryOperations

  class Query
    include QueryOperations

    def initialize(klass, query)
      @is_query = true
      @klass = klass
      @query = query
    end
  end

  class Middleware
    def initialize(name)
      @table_file = TableFile.new RubyDB::Config.file_root, name
      @id_name = :id
    end

    def field=(field)
      @raw_field = field
      @field = {@id_name => :integer}.merge field
    end

    def id_name=(id_name)
      @id_name = id_name
      if @field
        self.field = @raw_field
      end
    end

    def index=(index)
      @index = index
    end

    def select(query)
      if query.nil?
        io = open_content
        ret = io.read_all.map!{|r| externalize r}
        io.close
        ret
      elsif positions = search_index(query)
        # not implemented
      else
        q = internalize query
        records = nil
        open_content do |io|
          records = io.read_all
        end
        records.select! do |record|
          matched = true
          q.each_with_index do |value, i|
            unless value.nil? || QueryLogics.match(value, record[i])
              matched = false
              break
            end
          end
          matched
        end
        records.map! do |record|
          externalize record
        end
      end
    end

    def insert(record)
      record[@id_name] = current_sequence unless record.has_key? @id_name
      r = internalize record
      pos = nil
      open_content do |io|
        io.seek_last
        pos = io.tell
        io.write r
      end
      add_index pos, record
      record[@id_name]
    end

    def update(query, record)
      if positions = search_index(query)
        # not implemented
      else
        q = internalize query
        updated_records = {}
        open_content do |io|
          io.read_all.each_with_index do |r, j|
            matched = true
            q.each_with_index do |value, i|
              unless value.nil? || QueryLogics.match(value, r[i])
                matched = false
                break
              end
            end
            if matched
              updated = merge_external r, record
              io.seek j
              io.write updated
              updated_records[j] = updated
            end
          end
        end
        updated_records.each do |j, updated|
          edit_index j, updated
        end
      end
    end

    def delete(query)
      if query.nil?
        open_content do |io|
          io.delete_all
        end
      elsif positions = search_index(query)
        # not implemented
      else
        q = internalize query
        deleted_records = {}
        open_content do |io|
          io.read_all.each_with_index do |r, j|
            matched = true
            q.each_with_index do |value, i|
              unless value.nil? || QueryLogics.match(value, r[i])
                matched = false
                break
              end
            end
            if matched
              io.delete j
              deleted_records[j] = r
            end
          end
        end
        deleted_records.each do |j, deleted|
          remove_index j, deleted
        end
      end
    end

    def search_index(query)
      # not implemented
    end

    def add_index(pos, record)
      # not implemented
    end

    def edit_index(pos, record)
      # not implemented
    end

    def remove_index(pos, record)
      # not implemented
    end

    private
    def internalize(map_record)
      db_field.map do |name|
        map_record[name]
      end
    end

    def externalize(arr_record)
      map_record = {}
      db_field.each_with_index do |name, i|
        map_record[name] = arr_record[i]
      end
      map_record
    end

    def merge_external(arr_record, map_record)
      ret = arr_record.clone
      db_field.each_with_index do |name, i|
        if map_record.has_key? name
          ret[i] = map_record[name]
        end
      end
      ret
    end

    def db_field
      @db_field ||= db_context.first
    end

    def db_format
      @db_format ||= db_context.last
    end

    def db_context
      @db_context ||= begin
        db_field = nil
        RecordIO.open @table_file.header, @table_file.header_format do |io|
          db_field =
          if io.count == 0
            @field.keys.each do |k|
              io.write [k.to_s]
            end
          else
            io.read_all.flatten.map! &:to_sym
          end
        end
        db_format = db_field.map{|name| @field[name]}
        [db_field, db_format]
      end
    end

    def open_content(&block)
      RecordIO.open @table_file.content, db_format, &block
    end

    def open_index(name, format, &block)
      RecordIO.open(
        @table_file.index(name),
        @table_file.index_format(format),
        &block
      )
    end

    def current_sequence
      sequence = 1
      RecordIO.open @table_file.sequence, @table_file.sequence_format do |io|
        if io.count > 0
          sequence = io.read.first + 1
        end
        io.seek 0
        io.write [sequence]
      end
      sequence
    end

    class TableFile
      def initialize(path, name)
        @root = File.join path, name.to_s
        Dir.mkdir @root unless File.exist? @root
      end

      def header
        File.join @root, 'table.tbh'
      end

      def content
        File.join @root, 'table.tbl'
      end

      def sequence
        File.join @root, 'table.sqc'
      end

      def index(name)
        File.join @root, "#{name}.idx"
      end

      def header_format
        [:string]
      end

      def sequence_format
        [:integer]
      end

      def index_format(element)
        [element, :integer]
      end
    end

    class RecordIO
      PACK_FORMAT = {
        integer: 'H8',
        long: 'H16',
        string: 'Z256',
        boolean: 'c',
      }

      ELEMENT_SIZE = {
        integer: 4,
        long: 8,
        string: 256,
        boolean: 1,
      }

      DECODE = {
        integer: lambda{|s| s.to_i 16},
        long:    lambda{|s| s.to_i 16},
        boolean: lambda{|n| n == 1},
      }

      ENCODE = {
        integer: lambda{|n| int2hex n,  8},
        long:    lambda{|n| int2hex n, 16},
        boolean: lambda{|b| b ? 1 : 0},
      }

      EMPTY_BIN = [].pack ''

      def initialize(file, format)
        File.binwrite file, '' unless File.exist? file
        @f = File.open file, 'r+b'
        accept_format format
      end

      def self.open(file, format)
        if block_given?
          begin
            io = new file, format
            yield io
          ensure
            io && io.close
          end
        else
          new file, format
        end
      end

      def count
        @f.size / @record_size
      end

      def read
        decode @f.read @record_size
      end

      def read_all
        @f.seek 0
        ret = []
        ret << read until @f.eof?
        ret
      end

      def write(record)
        @f.write encode record
      end

      def insert(record)
        pos = @f.tell
        data = read_rest
        @f.seek pos
        write record
        @f.write data
      end

      def seek(n)
        @f.seek n * @record_size
      end

      def seek_last
        @f.seek 0, IO::SEEK_END
      end

      def tell
        @f.tell / @record_size
      end

      def delete(i)
        seek i + 1
        data = read_rest
        @f.truncate i * @record_size
        seek i
        @f.write data
      end

      def delete_all
        @f.truncate 0
      end

      def close
        @f and begin
          @f.close
          @f = nil
        end
      end

      private
      def accept_format(format)
        @format = format
        @pack_format = pack_format_for format
        @unpack_format = unpack_format_for format
        @record_size = record_size_for format
      end

      def pack_format_for(format)
        format.map{|type|
          if PACK_FORMAT.has_key? type
            PACK_FORMAT[type]
          elsif type.is_a? Integer
            "a#{type}"
          else
            raise Error, "bad type: #{type.inspect}"
          end
        }.join
      end

      def unpack_format_for(format)
        pack_format_for format
      end

      def record_size_for(format)
        format.map{|type|
          if ELEMENT_SIZE.has_key? type
            ELEMENT_SIZE[type]
          elsif type.is_a? Integer
            type
          else
            raise Error, "bad type: #{type.inspect}"
          end
        }.inject &:+
      end

      def decode(data)
        record = data.unpack @unpack_format
        @format.each_with_index do |element, index|
          if DECODE.has_key? element
            record[index] = DECODE[element].call record[index]
          end
        end
        record
      end

      def encode(record)
        record = record.clone
        @format.each_with_index do |element, index|
          if ENCODE.has_key? element
            record[index] = ENCODE[element].call record[index]
          end
        end
        record.pack @pack_format
      end

      def read_rest
        data = EMPTY_BIN.dup
        data << @f.read(@record_size) until @f.eof?
        data
      end

      def self.int2hex(i, size)
        hex = i.to_s 16
        if (l = hex.size) < size
          ret = '0' * size
          ret[-l, l] = hex
          ret
        elsif l > size
          hex[-size, size]
        else
          hex
        end
      end
    end

    module QueryLogics
      module_function

      def match(query, value)
        query == value
      end
    end
  end
end
