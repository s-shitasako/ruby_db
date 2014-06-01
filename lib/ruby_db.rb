class RubyDB
  class Error < StandardError; end

  module TableSettings
    def field(field)
      @field = field
    end

    def unique(keys)
      @unique = keys
    end

    def index(keys)
      @index = keys
    end
  end

  module QueryOperations
    def where(query)
    end

    def find_by(query)
    end

    def all
    end
    alias to_a all

    def first
    end

    def last
    end

    def destroy
    end

    private
    def klass
      @klass || self
    end

    def merge_query(query)
      (@query || []) << query
    end

    def inherit(query)
      Query.new klass, merge_query(query)
    end
  end

  def save
  end

  def destroy
  end

  extend TableSettings
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

      def index(name)
        File.join @root, "#{name}.idx"
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

      def initialize(file, format)
        @f = file
        accept_fotmat format
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

      def seek(n)
        @f.seek n * @record_size
      end

      def delete(i)
        records = read_all
        records.delete_at i
        @f.truncate 0
        # @f.seek 0
        records.each{|r| write r}
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
  end
end
