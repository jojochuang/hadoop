/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceAudience.Public
@InterfaceStability.Unstable
public abstract class INodeAttributeProvider {

  public static class AuthorizationContext {
    public String fsOwner;
    public String supergroup;
    public UserGroupInformation callerUgi;
    public INodeAttributes[] inodeAttrs;
    public INode[] inodes;
    public byte[][] pathByNameArr;
    public int snapshotId;
    public String path;
    public int ancestorIndex;
    public boolean doCheckOwner;
    public FsAction ancestorAccess;
    public FsAction parentAccess;
    public FsAction access;
    public FsAction subAccess;
    public boolean ignoreEmptyDir;
    public String operationName;

    public AuthorizationContext(
        String fsOwner,
        String supergroup,
        UserGroupInformation callerUgi,
        INodeAttributes[] inodeAttrs,
        INode[] inodes,
        byte[][] pathByNameArr,
        int snapshotId,
        String path,
        int ancestorIndex,
        boolean doCheckOwner,
        FsAction ancestorAccess,
        FsAction parentAccess,
        FsAction access,
        FsAction subAccess,
        boolean ignoreEmptyDir) {
      this.fsOwner = fsOwner;
      this.supergroup = supergroup;
      this.callerUgi = callerUgi;
      this.inodeAttrs = inodeAttrs;
      this.inodes = inodes;
      this.pathByNameArr = pathByNameArr;
      this.snapshotId = snapshotId;
      this.path = path;
      this.ancestorIndex = ancestorIndex;
      this.doCheckOwner = doCheckOwner;
      this.ancestorAccess = ancestorAccess;
      this.parentAccess = parentAccess;
      this.access = access;
      this.subAccess = subAccess;
      this.ignoreEmptyDir = ignoreEmptyDir;
    }

    public AuthorizationContext(
        String fsOwner,
        String supergroup,
        UserGroupInformation callerUgi,
        INodeAttributes[] inodeAttrs,
        INode[] inodes,
        byte[][] pathByNameArr,
        int snapshotId,
        String path,
        int ancestorIndex,
        boolean doCheckOwner,
        FsAction ancestorAccess,
        FsAction parentAccess,
        FsAction access,
        FsAction subAccess,
        boolean ignoreEmptyDir,
        String operationName) {
      this(fsOwner, supergroup, callerUgi, inodeAttrs, inodes,
          pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
          ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);
      this.operationName = operationName;
    }
  }

  /*public static class AuthorizationWithOperationName extends
      AuthorizationContext {
    String operationName;

    public AuthorizationWithOperationName(
        String fsOwner,
        String supergroup,
        UserGroupInformation callerUgi,
        INodeAttributes[] inodeAttrs,
        INode[] inodes,
        byte[][] pathByNameArr,
        int snapshotId,
        String path,
        int ancestorIndex,
        boolean doCheckOwner,
        FsAction ancestorAccess,
        FsAction parentAccess,
        FsAction access,
        FsAction subAccess,
        boolean ignoreEmptyDir,
        String operationName) {
      super(fsOwner, supergroup, callerUgi, inodeAttrs, inodes,
          pathByNameArr, snapshotId, path, ancestorIndex, doCheckOwner,
          ancestorAccess, parentAccess, access, subAccess, ignoreEmptyDir);
      this.operationName = operationName;
    }
  }*/

  /**
   * The AccessControlEnforcer allows implementations to override the
   * default File System permission checking logic enforced on a file system
   * object
   */
  public interface AccessControlEnforcer {

    /**
     * Checks permission on a file system object. Has to throw an Exception
     * if the filesystem object is not accessessible by the calling Ugi.
     * @param fsOwner Filesystem owner (The Namenode user)
     * @param supergroup super user geoup
     * @param callerUgi UserGroupInformation of the caller
     * @param inodeAttrs Array of INode attributes for each path element in the
     *                   the path
     * @param inodes Array of INodes for each path element in the path
     * @param pathByNameArr Array of byte arrays of the LocalName
     * @param snapshotId the snapshotId of the requested path
     * @param path Path String
     * @param ancestorIndex Index of ancestor
     * @param doCheckOwner perform ownership check
     * @param ancestorAccess The access required by the ancestor of the path.
     * @param parentAccess The access required by the parent of the path.
     * @param access The access required by the path.
     * @param subAccess If path is a directory, It is the access required of
     *                  the path and all the sub-directories. If path is not a
     *                  directory, there should ideally be no effect.
     * @param ignoreEmptyDir Ignore permission checking for empty directory?
     * @deprecated use {@link #checkPermissionWithContext(AuthorizationContext)}} instead
     * @throws AccessControlException
     */
    public abstract void checkPermission(String fsOwner, String supergroup,
        UserGroupInformation callerUgi, INodeAttributes[] inodeAttrs,
        INode[] inodes, byte[][] pathByNameArr, int snapshotId, String path,
        int ancestorIndex, boolean doCheckOwner, FsAction ancestorAccess,
        FsAction parentAccess, FsAction access, FsAction subAccess,
        boolean ignoreEmptyDir)
            throws AccessControlException;

    void checkPermissionWithContext(AuthorizationContext authzContext)
        throws AccessControlException;
  }
  /**
   * Initialize the provider. This method is called at NameNode startup
   * time.
   */
  public abstract void start();

  /**
   * Shutdown the provider. This method is called at NameNode shutdown time.
   */
  public abstract void stop();

  @Deprecated
  String[] getPathElements(String path) {
    path = path.trim();
    if (path.charAt(0) != Path.SEPARATOR_CHAR) {
      throw new IllegalArgumentException("It must be an absolute path: " +
          path);
    }
    int numOfElements = StringUtils.countMatches(path, Path.SEPARATOR);
    if (path.length() > 1 && path.endsWith(Path.SEPARATOR)) {
      numOfElements--;
    }
    String[] pathElements = new String[numOfElements];
    int elementIdx = 0;
    int idx = 0;
    int found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    while (found > -1) {
      if (found > idx) {
        pathElements[elementIdx++] = path.substring(idx, found);
      }
      idx = found + 1;
      found = path.indexOf(Path.SEPARATOR_CHAR, idx);
    }
    if (idx < path.length()) {
      pathElements[elementIdx] = path.substring(idx);
    }
    return pathElements;
  }

  @Deprecated
  public INodeAttributes getAttributes(String fullPath, INodeAttributes inode) {
    return getAttributes(getPathElements(fullPath), inode);
  }

  public abstract INodeAttributes getAttributes(String[] pathElements,
      INodeAttributes inode);

  public INodeAttributes getAttributes(byte[][] components,
      INodeAttributes inode) {
    String[] elements = new String[components.length];
    for (int i = 0; i < elements.length; i++) {
      elements[i] = DFSUtil.bytes2String(components[i]);
    }
    return getAttributes(elements, inode);
  }

  /**
   * Can be over-ridden by implementations to provide a custom Access Control
   * Enforcer that can provide an alternate implementation of the
   * default permission checking logic.
   * @param defaultEnforcer The Default AccessControlEnforcer
   * @return The AccessControlEnforcer to use
   */
  public AccessControlEnforcer getExternalAccessControlEnforcer(
      AccessControlEnforcer defaultEnforcer) {
    return defaultEnforcer;
  }
}
