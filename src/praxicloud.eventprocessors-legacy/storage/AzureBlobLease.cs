// Copyright (c) Christopher Clayton. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace praxicloud.eventprocessors.legacy.storage
{
	#region Using Clauses
	using System.Threading.Tasks;
	using Microsoft.Azure.Storage.Blob;
	using Newtonsoft.Json;
	#endregion

	/// <summary>
	/// A lease that leverages BLOB storage for ownership
	/// </summary>
	public class AzureBlobLease : CheckpointLease
	{
		#region Constructors
		/// <summary>
		/// Initializes a new instance of the type
		/// </summary>
		public AzureBlobLease()
		{
		}

		/// <summary>
		/// Initializes a new instance of the type
		/// </summary>
		/// <param name="partitionId">The partition id</param>
		/// <param name="blob">The BLOB that is used for managing ownership</param>
		public AzureBlobLease(string partitionId, CloudBlockBlob blob) : base(partitionId)
		{
			Blob = blob;
			IsOwned = blob.Properties.LeaseState == LeaseState.Leased;
		}

		/// <summary>
		/// Initializes a new instance of the type
		/// </summary>
		/// <param name="partitionId"></param>
		/// <param name="owner">The owner of the BLOBs</param>
		/// <param name="blob">The BLOB that is used for managing ownership</param>
		public AzureBlobLease(string partitionId, string owner, CloudBlockBlob blob) : base(partitionId)
		{
			Blob = blob;
			Owner = owner;
			IsOwned = blob.Properties.LeaseState == LeaseState.Leased;
		}

		/// <summary>
		/// Initializes a new instance of the type
		/// </summary>
		/// <param name="source">A lease that is used for the source of the contents</param>
		public AzureBlobLease(AzureBlobLease source) : base(source)
		{
			Blob = source.Blob;
			IsOwned = source.IsOwned;
		}

		/// <summary>
		/// Initializes a new instance of the type
		/// </summary>
		/// <param name="source">A lease that is used for the source of the contents</param>
		/// <param name="blob">The BLOB that is used for managing ownership</param>
		public AzureBlobLease(AzureBlobLease source, CloudBlockBlob blob) : base(source)
		{
			Blob = blob;
			IsOwned = blob.Properties.LeaseState == LeaseState.Leased;
		}
		#endregion
		#region Properties
		/// <summary>
		/// True if the lease currently owns the partition
		/// </summary>
		public bool IsOwned { get; }

		/// <summary>
		/// The BLOB that the lease is associated with
		/// </summary>
		[JsonIgnore]
		public CloudBlockBlob Blob { get; }
		#endregion
		#region Methods
		/// <summary>
		/// True if the lease has expired
		/// </summary>
		/// <returns></returns>
		public override Task<bool> IsExpired()
		{
			return Task.FromResult(!this.IsOwned);
		}
		#endregion
	}
}
