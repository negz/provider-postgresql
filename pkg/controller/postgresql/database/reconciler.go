/*
Copyright 2020 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/logging"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"

	"github.com/crossplane-contrib/provider-sql/apis/postgresql/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/postgresql"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/xsql"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errNoSecretRef  = "ProviderConfig does not reference a credentials Secret"
	errGetSecret    = "cannot get credentials Secret"

	errNotDatabase       = "managed resource is not a Database custom resource"
	errSelectDB          = "cannot select database"
	errCreateDB          = "cannot create database"
	errAlterDBOwner      = "cannot alter database owner"
	errAlterDBConnLimit  = "cannot alter database connection limit"
	errAlterDBAllowConns = "cannot alter database allow connections"
	errAlterDBIsTmpl     = "cannot alter database is template"
	errDropDB            = "cannot drop database"

	maxConcurrency = 5
)

// Setup adds a controller that reconciles Database managed resources.
func Setup(mgr ctrl.Manager, l logging.Logger) error {
	name := managed.ControllerName(v1alpha1.DatabaseGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.DatabaseGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newDB: postgresql.New}),
		managed.WithLogger(l.WithValues("controller", name)),
		managed.WithPollInterval(10*time.Minute),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Database{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrency,
		}).
		Complete(r)
}

type connector struct {
	kube  client.Client
	usage resource.Tracker
	newDB func(creds map[string][]byte, database string, sslmode string) xsql.DB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return nil, errors.New(errNotDatabase)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	// ProviderConfigReference could theoretically be nil, but in practice the
	// DefaultProviderConfig initializer will set it before we get here.
	pc := &v1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	// We don't need to check the credentials source because we currently only
	// support one source (PostgreSQLConnectionSecret), which is required and
	// enforced by the ProviderConfig schema.
	ref := pc.Spec.Credentials.ConnectionSecretRef
	if ref == nil {
		return nil, errors.New(errNoSecretRef)
	}

	s := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, s); err != nil {
		return nil, errors.Wrap(err, errGetSecret)
	}

	return &external{db: c.newDB(s.Data, "", pc.Spec.SslMode)}, nil
}

type external struct{ db xsql.DB }

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotDatabase)
	}

	// If the database exists, it will have all of these properties.
	observed := v1alpha1.DatabaseParameters{
		Owner:            new(string),
		Encoding:         new(string),
		LCCollate:        new(string),
		LCCType:          new(string),
		AllowConnections: new(bool),
		ConnectionLimit:  new(int),
		IsTemplate:       new(bool),
		Tablespace:       new(string),
	}

	query := "SELECT " +
		"pg_catalog.pg_get_userbyid(db.datdba), " +
		"pg_catalog.pg_encoding_to_char(db.encoding), " +
		"db.datcollate, " +
		"db.datctype, " +
		"db.datallowconn, " +
		"db.datconnlimit, " +
		"db.datistemplate, " +
		"ts.spcname " +
		"FROM pg_database AS db, pg_tablespace AS ts " +
		"WHERE db.datname=$1 AND db.dattablespace = ts.oid"

	err := c.db.Scan(ctx, xsql.Query{String: query, Parameters: []interface{}{meta.GetExternalName(cr)}},
		observed.Owner,
		observed.Encoding,
		observed.LCCollate,
		observed.LCCType,
		observed.AllowConnections,
		observed.ConnectionLimit,
		observed.IsTemplate,
		observed.Tablespace,
	)
	if xsql.IsNoRows(err) {
		return managed.ExternalObservation{ResourceExists: false}, nil
	}
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectDB)
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists: true,

		// NOTE(negz): The ordering is important here. We want to late init any
		// values that weren't supplied before we determine if an update is
		// required.
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, cr.Spec.ForProvider),
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) { //nolint:gocyclo
	// NOTE(negz): This is only a tiny bit over our cyclomatic complexity limit,
	// and more readable than if we refactored it to avoid the linter error.

	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotDatabase)
	}

	var b strings.Builder
	b.WriteString("CREATE DATABASE ")
	b.WriteString(pq.QuoteIdentifier(meta.GetExternalName(cr)))

	if cr.Spec.ForProvider.Owner != nil {
		b.WriteString(" OWNER ")
		b.WriteString(pq.QuoteIdentifier(*cr.Spec.ForProvider.Owner))
	}
	if cr.Spec.ForProvider.Template != nil {
		b.WriteString(" TEMPLATE ")
		b.WriteString(quoteIfIdentifier(*cr.Spec.ForProvider.Template))
	}
	if cr.Spec.ForProvider.Encoding != nil {
		b.WriteString(" ENCODING ")
		b.WriteString(quoteIfLiteral(*cr.Spec.ForProvider.Encoding))
	}
	if cr.Spec.ForProvider.LCCollate != nil {
		b.WriteString(" LC_COLLATE ")
		b.WriteString(quoteIfLiteral(*cr.Spec.ForProvider.LCCollate))
	}
	if cr.Spec.ForProvider.LCCType != nil {
		b.WriteString(" LC_CTYPE ")
		b.WriteString(quoteIfLiteral(*cr.Spec.ForProvider.LCCType))
	}
	if cr.Spec.ForProvider.Tablespace != nil {
		b.WriteString(" TABLESPACE ")
		b.WriteString(quoteIfIdentifier(*cr.Spec.ForProvider.Tablespace))
	}
	if cr.Spec.ForProvider.AllowConnections != nil {
		b.WriteString(fmt.Sprintf(" ALLOW_CONNECTIONS %t", *cr.Spec.ForProvider.AllowConnections))
	}
	if cr.Spec.ForProvider.ConnectionLimit != nil {
		b.WriteString(fmt.Sprintf(" CONNECTION LIMIT %d", *cr.Spec.ForProvider.ConnectionLimit))
	}
	if cr.Spec.ForProvider.IsTemplate != nil {
		b.WriteString(fmt.Sprintf(" IS_TEMPLATE %t", *cr.Spec.ForProvider.IsTemplate))
	}

	return managed.ExternalCreation{}, errors.Wrap(c.db.Exec(ctx, xsql.Query{String: b.String()}), errCreateDB)
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) { //nolint:gocyclo
	// NOTE(negz): This is only a tiny bit over our cyclomatic complexity limit,
	// and more readable than if we refactored it to avoid the linter error.

	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotDatabase)
	}

	if cr.Spec.ForProvider.Owner != nil {
		query := xsql.Query{String: fmt.Sprintf("ALTER DATABASE %s OWNER TO %s",
			pq.QuoteIdentifier(meta.GetExternalName(cr)),
			pq.QuoteIdentifier(*cr.Spec.ForProvider.Owner))}
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errAlterDBOwner)
		}
	}

	if cr.Spec.ForProvider.ConnectionLimit != nil {
		query := xsql.Query{String: fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT = %d",
			pq.QuoteIdentifier(meta.GetExternalName(cr)),
			*cr.Spec.ForProvider.ConnectionLimit)}
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errAlterDBConnLimit)
		}
	}

	if cr.Spec.ForProvider.AllowConnections != nil {
		query := xsql.Query{String: fmt.Sprintf("ALTER DATABASE %s ALLOW_CONNECTIONS %t",
			pq.QuoteIdentifier(meta.GetExternalName(cr)),
			*cr.Spec.ForProvider.AllowConnections)}
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errAlterDBAllowConns)
		}
	}

	if cr.Spec.ForProvider.IsTemplate != nil {
		query := xsql.Query{String: fmt.Sprintf("ALTER DATABASE %s IS_TEMPLATE %t",
			pq.QuoteIdentifier(meta.GetExternalName(cr)),
			*cr.Spec.ForProvider.IsTemplate)}
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errAlterDBIsTmpl)
		}
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return errors.New(errNotDatabase)
	}

	err := c.db.Exec(ctx, xsql.Query{String: "DROP DATABASE IF EXISTS " + pq.QuoteIdentifier(meta.GetExternalName(cr))})
	return errors.Wrap(err, errDropDB)
}

func upToDate(observed, desired v1alpha1.DatabaseParameters) bool {
	// Template is only used at create time.
	return cmp.Equal(desired, observed, cmpopts.IgnoreFields(v1alpha1.DatabaseParameters{}, "Template"))
}

func lateInit(observed v1alpha1.DatabaseParameters, desired *v1alpha1.DatabaseParameters) bool {
	li := false

	if desired.Owner == nil {
		desired.Owner = observed.Owner
		li = true
	}
	if desired.Encoding == nil {
		desired.Encoding = observed.Encoding
		li = true
	}
	if desired.LCCollate == nil {
		desired.LCCollate = observed.LCCollate
		li = true
	}
	if desired.LCCType == nil {
		desired.LCCType = observed.LCCType
		li = true
	}
	if desired.AllowConnections == nil {
		desired.AllowConnections = observed.AllowConnections
		li = true
	}
	if desired.ConnectionLimit == nil {
		desired.ConnectionLimit = observed.ConnectionLimit
		li = true
	}
	if desired.IsTemplate == nil {
		desired.IsTemplate = observed.IsTemplate
		li = true
	}
	if desired.Tablespace == nil {
		desired.Tablespace = observed.Tablespace
		li = true
	}

	return li
}

func quoteIfIdentifier(name string) string {
	if name == "DEFAULT" {
		return name
	}
	return pq.QuoteIdentifier(name)
}

func quoteIfLiteral(literal string) string {
	if literal == "DEFAULT" {
		return literal
	}
	return pq.QuoteLiteral(literal)
}
