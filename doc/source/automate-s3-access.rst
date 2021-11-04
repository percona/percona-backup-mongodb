.. _automate-s3-access:

***************************************
Automate access to S3 buckets for |PBM|
***************************************

When you run MongoDB and |PBM| using AWS EC2 instances, you can automate access to AWS S3 buckets for |PBM|. As of version 1.6.1, |PBM| uses the EC2 environment variables and metadata to access S3 buckets so that you don't have to explicitly specify the S3 credentials in the |PBM| configuration file. Thereby you control the access to your cloud infrastructure from a single place.

The steps to automate S3 buckets access for PBM are the following:

1.	Create the `IAM instance profile <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html>`_ and the permission policy within where you specify the access level that grants the access to S3 buckets. 
2.	Attach the IAM profile to an EC2 instance.
3.	Configure an S3 storage bucket and verify the connection from the EC2 instance to it.
4.	Provide the :ref:`remote storage information for PBM in a config file <backup-config>`. Leave the ``s3.credentials`` array empty

.. code-block:: yml

   storage:
     type: s3
     s3:
      region: <your-S3-region>
      bucket: <bucket-name>

.. note:: 

  If you specify S3 credentials, they override the EC2 instance environment variables and metadata, and are used for authentication instead. 

5. :ref:`start-pbm-agent`

.. seealso:: 

   AWS documentation: `How can I grant my Amazon EC2 instance access to an Amazon S3 bucket? <https://aws.amazon.com/premiumsupport/knowledge-center/ec2-instance-access-s3-bucket/>`_


.. include:: .res/replace.txt