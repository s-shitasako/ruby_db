require_relative '../lib/ruby_db'
require_relative 'config'

class Comment < RubyDB
  field({
    user_id: :integer,
    date: :long,
    content: :string
  })
  index :user_id, :date

  def read_users
    ReadMark.where(comment_id: id).to_a.map! do |mark|
      User.find_by id: mark.user_id
    end
  end
end

class User < RubyDB
  field({
    name: :string,
    email: :string,
    password: :string
  })
  index :email

  def self.authenticate(email, password)
    find_by email: email, password: password
  end

  def comments
    Comment.where(user_id: id).to_a
  end

  def do_comment(content)
    Comment.create user_id: id, date: Time.now.to_i, content: content
  end

  def mark_comment(comment_id)
    ReadMark.create user_id: id, comment_id: comment_id, read_at: Time.now.to_i
  end
end

class ReadMark < RubyDB
  field({
    user_id: :integer,
    comment_id: :integer,
    read_at: :long
  })
end
