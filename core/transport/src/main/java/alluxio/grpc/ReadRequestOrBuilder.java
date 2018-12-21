// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: block_worker.proto

package alluxio.grpc;

public interface ReadRequestOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.ReadRequest)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int64 block_id = 1;</code>
   */
  boolean hasBlockId();
  /**
   * <code>optional int64 block_id = 1;</code>
   */
  long getBlockId();

  /**
   * <code>optional int64 offset = 2;</code>
   */
  boolean hasOffset();
  /**
   * <code>optional int64 offset = 2;</code>
   */
  long getOffset();

  /**
   * <code>optional int64 length = 3;</code>
   */
  boolean hasLength();
  /**
   * <code>optional int64 length = 3;</code>
   */
  long getLength();

  /**
   * <pre>
   * Whether the block should be promoted before reading
   * </pre>
   *
   * <code>optional bool promote = 4;</code>
   */
  boolean hasPromote();
  /**
   * <pre>
   * Whether the block should be promoted before reading
   * </pre>
   *
   * <code>optional bool promote = 4;</code>
   */
  boolean getPromote();
}